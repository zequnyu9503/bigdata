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
package pers.yzq.timewindow.workload.realworld.twitter

import org.apache.spark.{SparkConf, SparkContext}
import pers.yzq.timewindow.PropertyProvider
import pers.yzq.timewindow.api.TimeWindowRDD

import scala.util.parsing.json.JSON

// scalastyle:off println

object Sample extends Serializable {

  private val hdfs = PropertyProvider.getString("workload.realworld.twitter.hdfs")

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Twitter Sample - " + System.currentTimeMillis())
      .setMaster("local")
    val sc = new SparkContext(conf)

    val originRDD = sc.textFile("C:\\Users\\YZQ\\Desktop\\30.json")
    val jsonRDD = originRDD.map(e => JSON.parseFull(e))
    // (T:Long, V:String)
    val valuableRDD = jsonRDD.map {
      case Some(map: Map[String, Any]) =>
        (map.get("id"), map.get("text"), map.get("timestamp_ms"), map.get("lang"))
      case _ => null
    }.filter(e => e._4 match {
      case Some(l: String) => if (l.equals("en")) true else false
      case _ => false
    }).map {
      case tag => (tag._3.get.toString.toLong, tag._2.get.toString)
      case _ => null
    }.cache()

    var intermediateRDD = sc.emptyRDD[(String, Long)]

    val itr = new TimeWindowRDD[Long, String](sc, 29499L, 29499L,
      (startTime: Long, endTime: Long) => {
        valuableRDD.filter(_._1 >= startTime).filter(_._1 <= endTime)
      }).setScope(1556605800659L, Long.MaxValue).iterator()

    while (itr.hasNext) {
      val windowRDD = itr.next()
      val textRDD = windowRDD.map(_._2)
      val freqRDD = textRDD.flatMap(e => e.split(" "))
          .map(e => (e, 1L)).reduceByKey(_ + _)
      intermediateRDD = intermediateRDD.union(freqRDD)
    }

    println(intermediateRDD.count())

    val resRDD = intermediateRDD.reduceByKey(_ + _).
      filter(_._2 > 5L).repartition(1)

    resRDD.saveAsTextFile("C:\\Users\\YZQ\\Desktop\\30-res")

    sc.stop()
  }
}
