/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package pers.yzq.timewindow.workload

import java.util.concurrent.Executors

import org.apache.spark.{SparkConf, SparkContext}

object Parallel {

  def main(args: Array[String]): Unit = {
    val threadpoolexecutor = Executors.newFixedThreadPool(2)
    val conf = new SparkConf().setAppName("Parallel")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(Seq(1, 1, 1))

    val job1 = new Runnable {
      override def run(): Unit = {
        println(s"Job1 ${rdd.count()}")
      }
    }

    val job2 = new Runnable {
      override def run(): Unit = {
        println(s"Job2 ${rdd.count()}")
      }
    }

    threadpoolexecutor.submit(job2)
    threadpoolexecutor.submit(job1)
  }
}
