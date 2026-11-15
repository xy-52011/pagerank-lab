#!/usr/bin/env bash
# 编译并打包 MapReduce 版 PageRank
set -e
cd "$(dirname "$0")"
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export HADOOP_HOME=${HADOOP_HOME:-/home/user/opt/hadoop-3.3.6}
CP=$(hadoop classpath)
rm -rf build && mkdir -p build/classes
javac -encoding UTF-8 -source 8 -target 8 -cp "$CP" -d build/classes \
  src/pagerank/PageRankMR.java
jar -cfe build/pagerank-mr.jar pagerank.PageRankMR -C build/classes .
echo "built: $(pwd)/build/pagerank-mr.jar"
