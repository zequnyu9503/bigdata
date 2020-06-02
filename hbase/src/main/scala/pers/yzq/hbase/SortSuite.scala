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
package pers.yzq.hbase

import org.apache.spark.{SparkConf, SparkContext}

object SortSuite {

  // scalastyle:off println
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SortSuite").setMaster("local")
    val sc = new SparkContext(conf)

    val origin = sc.parallelize(Seq(("c", (2, "...")), ("c", (1, "...")),
      ("b", (0, "...")), ("a", (3, "..."))))

    val sorted = origin.sortBy(_._1)
    println(s"Soted >> ${sorted.collect().mkString(", ")}")
  }
}
