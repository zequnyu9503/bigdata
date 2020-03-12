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
package pers.yzq.timewindow.workload.realworld.sougou

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

/*
数据格式:
访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对应同一个用户ID
 */

// scalastyle:off println
object UserPersistence {

  val r = "\t".r
  val spliter = ","
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("UserPersistence").setMaster("local")
    val sc = new SparkContext(conf)

    val day1 = sc.textFile("F:\\BaiduNetdiskDownload\\sougou\\access_log.20080601.decode.filter")

    // (V: Long, T: Long)
    val origin1 = day1.map(e => r.replaceAllIn(e, spliter)).
      map(e => e.split(spliter)).map(e => (e(1), sdf.parse("2008-06-01 " + e(0)).getTime))

    // per hour.
    val tw1 = origin1.filter(_._2 <= 1212249660000L)
    val trans1 = tw1.groupByKey(1).map(e => (e._1, e._2.toArray))

    val tw2 = origin1.filter(_._2 > 1212249660000L).filter(_._2 <=1212249720000L)
    val trans2 = tw2.groupByKey(1).map(e => (e._1, e._2.toArray))

    val trans3 = trans1.union(trans2).reduceByKey(_ ++: _).map(e => (e._1, e._2.max - e._2.min))

    println(trans3.collect().mkString("\n"))
  }
}
