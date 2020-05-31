#!/usr/bin/env bash
scala -classpath \
hbase/target/hbase-init.jar:\
hbase/target/dependency/* \
pers.yzq.hbase.Twitter