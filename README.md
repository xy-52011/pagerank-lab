# 实验三 PageRank 算法实现

大数据管理与分析课程实验三。内容包含两部分：

- 第一部分：HBase 常用操作（Shell 命令 + Java API 五个方法封装 + 关系表转 HBase）
- 第二部分：PageRank 算法实现（MapReduce 版与 Spark 版，伪分布式环境下迭代 10 次）

## 目录结构

```
pagerank-lab/
├── data/
│   ├── gen_dataset.py        # 数据集生成脚本（固定随机种子，可复现）
│   └── pages.txt             # 网页链接图：网页<TAB>出链集合
├── mapreduce/
│   ├── src/pagerank/PageRankMR.java   # MapReduce 版实现（迭代 PR + 悬挂节点处理 + 格式化输出）
│   ├── build.sh              # 编译打包脚本
│   └── build/pagerank-mr.jar # 编译产物（build.sh 生成）
├── spark/
│   └── pagerank_spark.py     # Spark(RDD) 版实现
├── hbase/
│   ├── src/hbasedemo/HBaseDemo.java   # HBase Java API：createTable/addRecord/scanColumn/modifyData/deleteRow
│   └── hbase_shell_demo.hb            # 等价的 HBase Shell 命令集合
├── scripts/
│   └── verify_pagerank.py    # 纯 Python 顺序实现，用于校验两套分布式结果
└── output/
    ├── mr_result.txt         # MapReduce 10 轮迭代结果
    ├── spark_result.txt      # Spark 10 轮迭代结果
    ├── verify.txt            # 校验结果
    └── logs/                 # 运行日志
```

## 环境

| 组件 | 版本 |
| --- | --- |
| 操作系统 | Ubuntu 22.04.5 LTS（2 vCPU / 4 GB 内存） |
| JDK | OpenJDK 11.0.33 |
| Hadoop | 3.3.6（HDFS + YARN 伪分布式） |
| Spark | 3.5.3（on YARN，Python 3.10） |
| HBase | 2.4.18（伪分布式，自带 ZooKeeper） |

## 运行步骤

```bash
# 0. 启动集群
hdfs --daemon start namenode
hdfs --daemon start datanode
yarn --daemon start resourcemanager
yarn --daemon start nodemanager

# 1. 生成数据集并上传 HDFS
cd data && python3 gen_dataset.py && cd ..
hdfs dfs -mkdir -p /pagerank/input
hdfs dfs -put -f data/pages.txt /pagerank/input/pages.txt

# 2. MapReduce 版（10 轮迭代）
cd mapreduce && ./build.sh && cd ..
hadoop jar mapreduce/build/pagerank-mr.jar /pagerank/input /pagerank/output 10
hdfs dfs -cat /pagerank/output/result/part-r-00000 > output/mr_result.txt

# 3. Spark 版（10 轮迭代）
spark-submit --master yarn --deploy-mode client \
  --driver-memory 512m --executor-memory 1g --executor-cores 2 --num-executors 1 \
  --conf spark.default.parallelism=4 \
  spark/pagerank_spark.py /pagerank/input /pagerank/output-spark 10
hdfs dfs -cat /pagerank/output-spark/part-* > output/spark_result.txt

# 4. 结果校验
python3 scripts/verify_pagerank.py

# 5. HBase 部分
hbase-daemon.sh start zookeeper && hbase-daemon.sh start master && hbase-daemon.sh start regionserver
hbase shell hbase/hbase_shell_demo.hb
javac -cp "$(hbase classpath)" -d hbase/build/classes hbase/src/hbasedemo/HBaseDemo.java
java -cp "hbase/build/classes:$(hbase classpath)" hbasedemo.HBaseDemo
```

## 算法说明

输入每行格式：`网页<TAB>出链网页1,出链网页2,...`，出链为空表示悬挂节点（dangling node）。
PR 迭代公式（d = 0.85，N 为网页总数，DM 为悬挂节点 PR 之和）：

```
PR(A) = (1 - d)/N + d * ( Σ PR(B)/L(B) + DM/N )
```

- MapReduce 版：每轮迭代由「悬挂节点质量统计」与「PR 更新」两个作业组成，固定迭代 10 轮后由格式化作业按 PR 降序输出。
- Spark 版：图结构 RDD 常驻缓存，每轮通过 `join` + `flatMap` 计算入链贡献，`leftOuterJoin` 合并后更新 PR，避免每轮重读 HDFS。

## 结果

两套实现输出格式均为 `(网页,PR值)`，PR 保留 10 位小数，节点数 100，PR 总和为 1。

```
(recovered_factory,…)   # 示例格式，本实验实际输出见 output/*_result.txt
```

Top10（MapReduce 与 Spark 完全一致）：

```
(Dictionary,0.0822607628) (Alaska,0.0712674503) (Algebra,0.0670482614) (Database,0.0623239481)
(Airbus,0.0495334023) (Absinthe,0.0354150483) (Achilles,0.0344260919) (Cancer,0.0341390166)
(Bread,0.0325613855) (Algorithm,0.0291517133)
```
