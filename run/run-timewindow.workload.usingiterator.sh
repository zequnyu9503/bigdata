#!/bin/bash
scala -classpath pers.yzq.timewindow.prefetcher.loader.UsingIterator ../timewindow/target/timewindow-init.jar \
-jar  /home/zc/yzq/libs/target/dependency/hadoop-common-3.1.0.jar, \
/home/zc/yzq/libs/target/dependency/hadoop-mapreduce-client-core-2.7.7.jar