#!/usr/bin/env bash
scala -classpath \
-Xmx 16g \
hbase/target/hbase-init.jar:\
hbase/target/dependency/* \
pers.yzq.hbase.Twitter