#!/bin/bash
scala -classpath \
../timewindow/target/timewindow-init.jar: \
../timewindow/target/timewindow-init-jar-with-dependencies.jar \
pers.yzq.timewindow.prefetcher.loader.UsingIterator