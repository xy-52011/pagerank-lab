package hbasedemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 实验第一部分：HBase 常用 Java API 封装。
 * createTable / addRecord / scanColumn / modifyData / deleteRow 五个方法，
 * 以及把关系型数据库中的 Student、Course、SC 三张表转换到 HBase 的示例。
 */
public class HBaseDemo {

    private static Connection conn;
    private static Admin admin;

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        conn = ConnectionFactory.createConnection(conf);
        admin = conn.getAdmin();

        // ---- 关系型表转 HBase ----
        // Student 表：rowkey = 学号（原数据中 2015003 重复，Lisi 记为 2015004）
        createTable("student", new String[]{"info"});
        addRecord("student", "2015001", new String[]{"info:name", "info:sex", "info:age"},
                new String[]{"Zhangsan", "male", "23"});
        addRecord("student", "2015003", new String[]{"info:name", "info:sex", "info:age"},
                new String[]{"Mary", "female", "22"});
        addRecord("student", "2015004", new String[]{"info:name", "info:sex", "info:age"},
                new String[]{"Lisi", "male", "24"});

        // Course 表：rowkey = 课程号
        createTable("course", new String[]{"info"});
        addRecord("course", "123001", new String[]{"info:name", "info:credit"},
                new String[]{"Math", "2.0"});
        addRecord("course", "123002", new String[]{"info:name", "info:credit"},
                new String[]{"Computer Science", "5.0"});
        addRecord("course", "123003", new String[]{"info:name", "info:credit"},
                new String[]{"English", "3.0"});

        // SC 表：rowkey = 学号+课程号，不同课程成绩放在同一列族的不同列
        createTable("sc", new String[]{"Score"});
        addRecord("sc", "2015001-123001", new String[]{"Score:Math"}, new String[]{"86"});
        addRecord("sc", "2015001-123003", new String[]{"Score:English"}, new String[]{"69"});
        addRecord("sc", "2015002-123002", new String[]{"Score:Computer Science"}, new String[]{"77"});
        addRecord("sc", "2015002-123003", new String[]{"Score:English"}, new String[]{"99"});
        addRecord("sc", "2015003-123001", new String[]{"Score:Math"}, new String[]{"98"});
        addRecord("sc", "2015003-123002", new String[]{"Score:Computer Science"}, new String[]{"95"});

        System.out.println("---- 建表结果 ----");
        listTables();

        System.out.println("---- 全表扫描 student ----");
        printAll("student");

        System.out.println("---- scanColumn: student 表 info 列族 ----");
        scanColumn("student", "info");

        System.out.println("---- scanColumn: sc 表 Score:Math 列 ----");
        scanColumn("sc", "Score:Math");

        System.out.println("---- modifyData: 把 2015001 的年龄改成 24 ----");
        modifyData("student", "2015001", "info:age", "24");
        printAll("student");

        System.out.println("---- deleteRow: 删除 student 中 2015004 这一行 ----");
        deleteRow("student", "2015004");
        printAll("student");

        System.out.println("---- 统计行数 ----");
        countRows("student");
        countRows("course");
        countRows("sc");

        admin.close();
        conn.close();
    }

    /**
     * 创建表；若同名表已存在，先删除再创建。
     *
     * @param tableName 表名
     * @param fields    列族名称数组
     */
    public static void createTable(String tableName, String[] fields) throws IOException {
        TableName tn = TableName.valueOf(tableName);
        if (admin.tableExists(tn)) {
            if (admin.isTableEnabled(tn)) {
                admin.disableTable(tn);
            }
            admin.deleteTable(tn);
            System.out.println("[createTable] 已删除原有表 " + tableName);
        }
        TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tn);
        for (String cf : fields) {
            builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(cf));
        }
        TableDescriptor desc = builder.build();
        admin.createTable(desc);
        System.out.println("[createTable] 创建表 " + tableName + " 列族 " + String.join(",", fields));
    }

    /**
     * 向指定行写入数据；fields 形如 "columnFamily:column"。
     */
    public static void addRecord(String tableName, String row, String[] fields, String[] values)
            throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(row));
        for (int i = 0; i < fields.length; i++) {
            String[] kv = fields[i].split(":");
            String family = kv[0];
            String qualifier = kv.length > 1 ? kv[1] : "";
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(values[i]));
        }
        table.put(put);
        table.close();
        System.out.println("[addRecord] " + tableName + " row=" + row + " 写入 " + fields.length + " 个单元格");
    }

    /**
     * 浏览某一列（族）的数据：参数为列族时列出该列族下所有列限定符；为具体列时只打印该列。
     * 该单元格不存在时输出 null。
     */
    public static void scanColumn(String tableName, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        String[] kv = column.split(":");
        if (kv.length == 1) {
            scan.addFamily(Bytes.toBytes(kv[0]));
        } else {
            scan.addColumn(Bytes.toBytes(kv[0]), Bytes.toBytes(kv[1]));
        }
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            String row = Bytes.toString(r.getRow());
            StringBuilder sb = new StringBuilder("[" + row + "] ");
            boolean hasValue = false;
            for (Cell cell : r.rawCells()) {
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                if (kv.length == 1 || (family.equals(kv[0]) && qualifier.equals(kv[1]))) {
                    sb.append(family).append(":").append(qualifier).append("=").append(value).append("  ");
                    hasValue = true;
                }
            }
            if (hasValue) {
                System.out.println(sb.toString().trim());
            } else {
                System.out.println("[" + row + "] null");
            }
        }
        scanner.close();
        table.close();
    }

    /**
     * 修改指定单元格的值。
     */
    public static void modifyData(String tableName, String row, String column, String value)
            throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        String[] kv = column.split(":");
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(kv[0]), Bytes.toBytes(kv[1]), Bytes.toBytes(value));
        table.put(put);
        table.close();
        System.out.println("[modifyData] " + tableName + " row=" + row + " " + column + " <- " + value);
    }

    /**
     * 删除指定行。
     */
    public static void deleteRow(String tableName, String row) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        table.delete(new Delete(Bytes.toBytes(row)));
        table.close();
        System.out.println("[deleteRow] " + tableName + " row=" + row + " 已删除");
    }

    public static void printAll(String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            StringBuilder sb = new StringBuilder(Bytes.toString(r.getRow())).append(" -> ");
            for (Cell cell : r.rawCells()) {
                sb.append(Bytes.toString(CellUtil.cloneFamily(cell))).append(":")
                  .append(Bytes.toString(CellUtil.cloneQualifier(cell))).append("=")
                  .append(Bytes.toString(CellUtil.cloneValue(cell))).append(" ");
            }
            System.out.println(sb.toString().trim());
        }
        scanner.close();
        table.close();
    }

    public static void countRows(String tableName) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        int count = 0;
        for (Result ignored : scanner) {
            count++;
        }
        scanner.close();
        table.close();
        System.out.println("[count] " + tableName + " 行数 = " + count);
    }

    public static void listTables() throws IOException {
        for (TableDescriptor td : admin.listTableDescriptors()) {
            System.out.println("表: " + td.getTableName().getNameAsString()
                    + " 列族: " + td.getColumnFamilies().length);
        }
    }
}
