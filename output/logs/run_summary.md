# 运行记录摘要

集群：Ubuntu 22.04.5 / JDK 11.0.33 / Hadoop 3.3.6（HDFS+YARN 伪分布式）/ Spark 3.5.3 on YARN / HBase 2.4.18

## MapReduce 版（10 轮迭代，21 个作业）

```
[info] 节点数 N = 100
[iter 1] dangling mass = 0.08
[iter 2] dangling mass = 0.03316500000000001
...
[iter 10] dangling mass = 0.03305413351566612
[top10] (Dictionary,0.0822607628) (Alaska,0.0712674503) (Algebra,0.0670482614) ...
```

总耗时 17 分 50 秒（1070 秒），输入 /pagerank/input，输出 /pagerank/output/result。

## Spark 版（10 轮迭代，4 个分区）

```
[info] 节点数 N = 100
[iter 1] dangling mass = 0.080000000000  top3 = Alaska:0.0568633333, Algebra:0.0530383333, Dictionary:0.0400050000
...
[iter 10] dangling mass = 0.033054133516  top3 = Dictionary:0.0822607628, Alaska:0.0712674503, Algebra:0.0670482614
[info] PR 总和 = 1.0000000000 （应等于节点数 100）
```

总耗时 2 分 54 秒（174 秒），输出 /pagerank/output-spark。

## 一致性

- MapReduce 与 Spark 的 100 个网页 PR 值（保留 10 位小数）完全一致，仅并列值的输出先后不同。
- 两套实现与纯 Python 顺序参考实现的最大偏差为 4.906×10⁻¹¹。

## 环境问题与处理

1. ZooKeeper 的 admin 端口默认 8080 与系统已占用端口冲突，改为 8085 后正常启动（hbase.zookeeper.property.admin.serverPort=8085）。
2. HBase 2.4.18 与 Hadoop 3.3.6 的异步 WAL 不兼容，Regionserver 报 `object is not an instance of declaring class`，设置 `hbase.wal.provider=filesystem` 后恢复正常。
3. 初始 PR 需要 1/N，第一版实现未先统计节点数导致 PR 初值为 0，改为驱动端先统计节点数（countNodes）后解决。
