#!/bin/bash
scala -classpath \
timewindow/target/timewindow-init.jar:\
timewindow/target/dependency/* \
pers.yzq.timewindow.prefetcher.loader.UsingIterator 0