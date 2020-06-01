#!/usr/bin/env bash
libs_dir="hdfs://centos3:9000/libs"
log_path="/home/zc/service/spark-2.4.4/conf/log4j.properties"
spark-submit \
--master spark://centos3:7079 \
--executor-memory 12g \
--executor-cores 6 \
--driver-cores 4 \
--driver-memory 8g \
--class pers.yzq.hbase.Twitter \
--driver-java-options "-Dlog4j.configuration=file:${log_path}" \
--jars \
${libs_dir}/hbase-common-2.1.4.jar,\
${libs_dir}/hbase-client-2.1.4.jar,\
${libs_dir}/hbase-mapreduce-2.1.4.jar,\
${libs_dir}/hbase-protocol-2.1.4.jar,\
${libs_dir}/hbase-protocol-shaded-2.1.4.jar,\
${libs_dir}/hbase-shaded-miscellaneous-2.1.0.jar,\
${libs_dir}/hbase-shaded-netty-2.1.0.jar,\
${libs_dir}/hbase-shaded-protobuf-2.1.0.jar,\
${libs_dir}/htrace-core-3.1.0-incubating.jar,\
${libs_dir}/fastjson-1.2.35.jar \
hbase/target/hbase-init.jar