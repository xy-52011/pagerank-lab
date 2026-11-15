# -*- coding: utf-8 -*-
"""独立校验：用纯 Python 顺序实现 PageRank，与 MapReduce / Spark 的输出比对。

用法: python3 scripts/verify_pagerank.py
"""
import re

DAMPING = 0.85
ITERS = 10


def load(path):
    graph = {}
    for line in open(path, encoding="utf-8"):
        line = line.rstrip("\n")
        if not line.strip():
            continue
        parts = line.split("\t")
        page = parts[0]
        links = [t for t in parts[1].split(",") if t] if len(parts) > 1 else []
        graph[page] = links
    for links in list(graph.values()):
        for t in links:
            graph.setdefault(t, [])
    return graph


def compute(graph):
    n = len(graph)
    pr = {p: 1.0 / n for p in graph}
    for _ in range(ITERS):
        dmass = sum(pr[p] for p in graph if not graph[p])
        new = {}
        for p in graph:
            s = sum(pr[q] / len(graph[q]) for q in graph if p in graph[q])
            new[p] = (1.0 - DAMPING) / n + DAMPING * (s + dmass / n)
        pr = new
    return pr


def read_result(path):
    out = {}
    for line in open(path, encoding="utf-8"):
        for page, val in re.findall(r"\((.*?),\d+\.\d+\)", line):
            out[page] = float(val)
    return out


def main():
    graph = load("data/pages.txt")
    ref = compute(graph)
    mr = read_result("output/mr_result.txt")
    sp = read_result("output/spark_result.txt")
    print("节点数=%d 边数=%d 悬挂节点=%d" % (
        len(graph), sum(len(v) for v in graph.values()),
        sum(1 for v in graph.values() if not v)))
    print("参考实现 PR 总和 = %.10f" % sum(ref.values()))
    for name, res in (("MapReduce", mr), ("Spark", sp)):
        diff = max(abs(ref[p] - res[p]) for p in ref)
        print("%s: 与参考实现最大偏差 = %.3e，节点数=%d" % (name, diff, len(res)))
    diff_ms = max(abs(mr[p] - sp[p]) for p in mr)
    print("MapReduce 与 Spark 之间最大偏差 = %.3e" % diff_ms)
    top = sorted(ref.items(), key=lambda kv: -kv[1])[:10]
    print("参考实现 Top10:")
    for p, v in top:
        print("  (%s,%.10f)" % (p, v))
    vals = sorted(ref.values())
    print("PR 最小值=%.10f 最大值=%.10f 中位数=%.10f" % (vals[0], vals[-1], vals[len(vals) // 2]))
    same = sum(1 for v in vals if abs(v - vals[0]) < 1e-12)
    print("与最小值相同的网页数量（无入链页面）= %d" % same)


if __name__ == "__main__":
    main()
