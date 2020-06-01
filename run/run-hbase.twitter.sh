#!/usr/bin/env bash
scala \
#-J-Xmx 16g \
-classpath hbase/target/hbase-init.jar:hbase/target/dependency/* \
pers.yzq.hbase.Twitter