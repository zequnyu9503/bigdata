#!/bin/bash
spark-submit \
--master spark://centos3:7079 \
--executor-memory 12G \
--executor-cores 4 \
--driver-memory 12G \
--driver-cores 4 \
--class pers.yzq.timewindow.workload.WordCount \
../timewindow/target/