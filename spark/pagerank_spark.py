# -*- coding: utf-8 -*-
"""PageRank 的 Spark(RDD) 实现。

用法:
    spark-submit --master yarn spark/pagerank_spark.py <input> <output> [iterations]

输入每行: "网页<TAB>该网页链接到的网页集合(英文逗号分隔)"，出链为空表示悬挂节点。
输出每行: "(网页,PR)"，PR 保留 10 位小数，按 PR 降序排列。
"""
import sys

from pyspark import SparkConf, SparkContext

DAMPING = 0.85


def parse(line):
    parts = line.rstrip("\n").split("\t")
    page = parts[0].strip()
    links = []
    if len(parts) > 1 and parts[1].strip():
        links = [t.strip() for t in parts[1].split(",") if t.strip()]
    return page, links


def main():
    if len(sys.argv) < 3:
        print("Usage: pagerank_spark.py <input> <output> [iterations]")
        sys.exit(2)
    inp, out = sys.argv[1], sys.argv[2]
    iterations = int(sys.argv[3]) if len(sys.argv) > 3 else 10

    sc = SparkContext(conf=SparkConf().setAppName("PageRank-Spark")
                      .set("spark.default.parallelism", "4"))

    # 图结构：(网页, 出链列表)；链接目标若只被引用没有出链，也要补成独立节点
    graph = sc.textFile(inp).filter(lambda l: l.strip()).map(parse)
    graph = graph.flatMap(
        lambda kv: [(kv[0], kv[1])] + [(t, []) for t in kv[1]]
    ).reduceByKey(lambda a, b: a if a else b).repartition(4).cache()

    size = graph.count()
    print("[info] 节点数 N = %d" % size)

    # PR 与图结构分开保存：ranks = (网页, PR)
    ranks = graph.map(lambda kv: (kv[0], 1.0 / size)).cache()

    for i in range(1, iterations + 1):
        # 每个页面把 PR 按出度均分给它的出链目标
        contribs = graph.join(ranks).flatMap(
            lambda kv: [(t, kv[1][1] / len(kv[1][0])) for t in kv[1][0]]
        ).reduceByKey(lambda a, b: a + b)

        # 悬挂节点的 PR 不通过链接传递，按 PR 公式均摊给全部 N 个页面
        dmass = graph.filter(lambda kv: not kv[1]).join(ranks) \
                     .map(lambda kv: kv[1][1]).sum()

        newranks = ranks.leftOuterJoin(contribs).mapValues(
            lambda v: (1.0 - DAMPING) / size
                      + DAMPING * ((v[1] or 0.0) + dmass / size)
        ).cache()

        top = newranks.takeOrdered(3, key=lambda kv: -kv[1])
        print("[iter %d] dangling mass = %.12f  top3 = %s"
              % (i, dmass, ", ".join("%s:%.10f" % (p, r) for p, r in top)))
        ranks.unpersist()
        ranks = newranks

    total = ranks.map(lambda kv: kv[1]).sum()
    print("[info] PR 总和 = %.10f （应等于节点数 %d）" % (total, size))

    result = ranks.map(lambda kv: ("(%s,%.10f)" % (kv[0], kv[1]), kv[1])) \
                  .sortBy(lambda kv: -kv[1]) \
                  .map(lambda kv: kv[0])
    result.coalesce(1).saveAsTextFile(out)

    print("[top10] " + " ".join(result.take(10)))
    sc.stop()


if __name__ == "__main__":
    main()
