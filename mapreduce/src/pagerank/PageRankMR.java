package pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * PageRank 的 MapReduce 实现（伪分布式 HDFS + YARN）。
 *
 * 迭代格式：每行 "page \t pr \t links"，links 为出链集合，用逗号分隔；
 * 出链为空表示悬挂节点（dangling node）。
 *
 * 每次迭代分两个作业：
 *   1) DanglingMass：把所有悬挂节点的 PR 求和，得到 dmass；
 *   2) RankUpdate：按 PR(A) = (1-d)/N + d*(Σ PR(B)/L(B) + dmass/N) 更新全部节点。
 * 迭代固定的 10 轮后，由 Format 作业按 PR 值降序输出 "(page,PR)"，PR 保留 10 位小数。
 */
public class PageRankMR extends Configured implements Tool {

    private static final double DAMPING = 0.85;
    private static final int ITERATIONS = 10;

    public static class InitMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }
            String[] parts = line.split("\t", -1);
            String page = parts[0];
            String links = parts.length > 1 ? parts[1] : "";
            context.write(new Text(page), new Text("#" + links));
            if (!links.isEmpty()) {
                for (String target : links.split(",")) {
                    if (!target.isEmpty()) {
                        context.write(new Text(target), new Text("$"));
                    }
                }
            }
        }
    }

    public static class InitReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String links = "";
            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("#")) {
                    links = s.substring(1);
                }
            }
            long n = context.getConfiguration().getLong("pagerank.node.count", 0L);
            double init = n == 0 ? 0.0 : 1.0 / (double) n;
            context.write(key, new Text(String.valueOf(init) + "\t" + links));
        }
    }

    public static class DangMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t", -1);
            if (parts.length < 3) {
                return;
            }
            if (parts[2].trim().isEmpty()) {
                context.write(new Text("dangling"), new Text(parts[1]));
            }
        }
    }

    public static class DangReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            for (Text v : values) {
                sum += Double.parseDouble(v.toString());
            }
            context.write(new Text("dmass"), new Text(String.valueOf(sum)));
        }
    }

    public static class IterMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t", -1);
            if (parts.length < 3) {
                return;
            }
            String page = parts[0];
            double pr = Double.parseDouble(parts[1]);
            String links = parts[2];
            context.write(new Text(page), new Text("#" + links));
            if (!links.isEmpty()) {
                String[] targets = links.split(",");
                int degree = targets.length;
                if (degree > 0) {
                    double share = pr / (double) degree;
                    for (String t : targets) {
                        if (!t.isEmpty()) {
                            context.write(new Text(t), new Text(String.valueOf(share)));
                        }
                    }
                }
            }
        }
    }

    public static class IterReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            long n = conf.getLong("pagerank.node.count", 0L);
            double dmass = Double.parseDouble(conf.get("pagerank.dangling.mass", "0"));
            String links = "";
            double inSum = 0.0;
            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("#")) {
                    links = s.substring(1);
                } else {
                    inSum += Double.parseDouble(s);
                }
            }
            double newPr = (1.0 - DAMPING) / (double) n
                    + DAMPING * (inSum + dmass / (double) n);
            context.write(key, new Text(String.valueOf(newPr) + "\t" + links));
        }
    }

    public static class FormatMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t", -1);
            if (parts.length < 2) {
                return;
            }
            context.write(new Text("all"), new Text(parts[0] + "\t" + parts[1]));
        }
    }

    public static class FormatReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String[]> rows = new ArrayList<String[]>();
            for (Text v : values) {
                String[] f = v.toString().split("\t");
                rows.add(new String[]{f[0], f[1]});
            }
            Collections.sort(rows, new Comparator<String[]>() {
                public int compare(String[] a, String[] b) {
                    return Double.compare(Double.parseDouble(b[1]), Double.parseDouble(a[1]));
                }
            });
            for (String[] r : rows) {
                context.write(new Text("(" + r[0] + "," + String.format("%.10f", Double.parseDouble(r[1])) + ")"),
                        new Text(""));
            }
        }
    }

    private static Job baseJob(Configuration conf, String name, Path in, Path out) throws IOException {
        // 数据规模较小，开启 uber 作业模式，让 map/reduce 在 ApplicationMaster 内运行
        conf.setBoolean("mapreduce.job.ubertask.enable", true);
        Job job = Job.getInstance(conf, name);
        job.setJarByClass(PageRankMR.class);
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job, out);
        return job;
    }

    /** 统计输入中的网页总数（含仅作为链接目标出现的网页），作为 PR 初始值 1/N 的基数。 */
    private static long countNodes(Configuration conf, Path input) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Set<String> nodes = new HashSet<String>();
        FileStatus[] files = fs.globStatus(new Path(input, "*"));
        for (FileStatus f : files) {
            if (!f.isFile()) {
                continue;
            }
            FSDataInputStream in = fs.open(f.getPath());
            java.io.BufferedReader reader = new java.io.BufferedReader(
                    new java.io.InputStreamReader(in, "UTF-8"));
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) {
                    continue;
                }
                String[] parts = line.split("\t", -1);
                nodes.add(parts[0]);
                if (parts.length > 1 && !parts[1].isEmpty()) {
                    for (String t : parts[1].split(",")) {
                        if (!t.trim().isEmpty()) {
                            nodes.add(t.trim());
                        }
                    }
                }
            }
            IOUtils.closeStream(reader);
        }
        return nodes.size();
    }

    private static double readDanglingMass(Configuration conf, Path dir) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path part = new Path(dir, "part-r-00000");
        if (!fs.exists(part)) {
            return 0.0;
        }
        FSDataInputStream in = fs.open(part);
        String line = in.readLine();
        in.close();
        String[] f = line.split("\t");
        return Double.parseDouble(f[1]);
    }

    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: PageRankMR <input> <output> <iterations>");
            return 2;
        }
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        int iterations = args.length > 2 ? Integer.parseInt(args[2]) : ITERATIONS;

        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            fs.delete(output, true);
        }
        Path work = new Path(output, "_work");
        fs.mkdirs(work);

        // 1. 初始化：补全所有节点、构造初始 PR = 1/N
        long n = countNodes(conf, input);
        if (n == 0) {
            System.err.println("[error] 输入为空");
            return 1;
        }
        System.out.println("[info] 节点数 N = " + n);
        Configuration initConf = new Configuration(conf);
        initConf.setLong("pagerank.node.count", n);
        Job init = baseJob(initConf, "pagerank-init", input, new Path(work, "iter-0"));
        init.setMapperClass(InitMapper.class);
        init.setReducerClass(InitReducer.class);
        init.setOutputKeyClass(Text.class);
        init.setOutputValueClass(Text.class);
        if (!init.waitForCompletion(true)) {
            return 1;
        }

        Path current = new Path(work, "iter-0");
        for (int i = 1; i <= iterations; i++) {
            // 2. 悬挂节点 PR 总量
            Path dangOut = new Path(work, "dmass-" + i);
            Job dang = baseJob(conf, "pagerank-dmass-" + i, current, dangOut);
            dang.setMapperClass(DangMapper.class);
            dang.setReducerClass(DangReducer.class);
            dang.setNumReduceTasks(1);
            dang.setOutputKeyClass(Text.class);
            dang.setOutputValueClass(Text.class);
            if (!dang.waitForCompletion(true)) {
                return 1;
            }
            double dmass = readDanglingMass(conf, dangOut);
            System.out.println("[iter " + i + "] dangling mass = " + dmass);

            // 3. 一轮 PR 更新
            Path next = new Path(work, "iter-" + i);
            Configuration c = new Configuration(conf);
            c.setLong("pagerank.node.count", n);
            c.set("pagerank.dangling.mass", String.valueOf(dmass));
            Job iter = baseJob(c, "pagerank-iter-" + i, current, next);
            iter.setMapperClass(IterMapper.class);
            iter.setReducerClass(IterReducer.class);
            iter.setOutputKeyClass(Text.class);
            iter.setOutputValueClass(Text.class);
            if (!iter.waitForCompletion(true)) {
                return 1;
            }
            current = next;
        }

        // 4. 格式化输出：(page,PR)，PR 保留 10 位小数，按 PR 降序
        Job format = baseJob(new Configuration(conf), "pagerank-format", current, new Path(output, "result"));
        format.setMapperClass(FormatMapper.class);
        format.setReducerClass(FormatReducer.class);
        format.setNumReduceTasks(1);
        format.setOutputKeyClass(Text.class);
        format.setOutputValueClass(Text.class);
        if (!format.waitForCompletion(true)) {
            return 1;
        }

        // 打印前 10 名到控制台，便于核对
        FileSystem fs2 = FileSystem.get(conf);
        Path res = new Path(output, "result/part-r-00000");
        FSDataInputStream in = fs2.open(res);
        StringBuilder sb = new StringBuilder();
        String line;
        int cnt = 0;
        while ((line = in.readLine()) != null && cnt < 10) {
            sb.append(line).append(' ');
            cnt++;
        }
        in.close();
        System.out.println("[top10] " + sb.toString().trim());
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new Configuration(), new PageRankMR(), args);
        System.exit(rc);
    }
}
