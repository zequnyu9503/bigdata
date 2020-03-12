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
package pers.yzq.timewindow

import java.text.SimpleDateFormat
import java.util.concurrent.Executors

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.FunSuite

class Simple extends FunSuite{

  var s: String = _

  private val threadpoolexecutor = Executors.newFixedThreadPool(2)

  test("_ null") {
    if (s.eq(null)) {
      // scalastyle:off println
      System.err.println("null")
    }
  }

  test("Parallel") {
    val conf = new SparkConf().setAppName("Parallel").setMaster("local")
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

  test("match") {
    var str = "a     b"
    val pattern = "[ ]*".r
    str = pattern.replaceAllIn(str, "")
    println(str)
  }

  test("Date") {
    val conf = new SparkConf().setAppName("UserPersistence").setMaster("local")
    val sc = new SparkContext(conf)

    val days = Array("01", "02", "03", "04", "05", "06", "07", "08", "09",
      "11", "12", "13", "14", "15", "16", "17",
      "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28", "29")

    var count = 0L
    for (i <- days.indices) {
      val dayx = sc.textFile("F:\\BaiduNetdiskDownload\\sougou\\access_log.200806" +
        days(i) + ".decode.filter")
      count += dayx.count()
    }

    println(count)
  }

  test("groupByKey") {
    val conf = new SparkConf().setAppName("simple-" + System.currentTimeMillis()).setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Array((1, 10), (2, 20), (1, 30), (1, 40), (2, 50)))
    val trans1 = rdd1.groupByKey(2).map(e => (e._1, e._2.toArray))

    val rdd2 = sc.parallelize(Array((1, 100), (2, 200), (1, 300), (1, 400), (2, 500)))
    val trans2 = rdd2.groupByKey(2).map(e => (e._1, e._2.toArray))

    val res = trans1.union(trans2).reduceByKey(_ ++: _).map(e => (e._1, e._2.max - e._2.min))
    println(res.collect().mkString(","))
  }

  test("NumberFormatException") {
    println("24011165741831064224".toLong)
  }

  test("load from file ") {

  }
}
