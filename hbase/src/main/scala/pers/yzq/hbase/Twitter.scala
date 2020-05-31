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

import java.util

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Twitter extends BulkLoad {

  tableName = PropertyProvider.getString("hbase.bulkload.tablename")
  columnFamily = PropertyProvider.getString("hbase.bulkload.columnfamily")
  columnQualify = PropertyProvider.getString("hbase.bulkload.columnqualify")
  hfile = PropertyProvider.getString("hbase.bulkload.hfile")
  hadoop_file = PropertyProvider.getString("hbase.bulkload.hadoopfile")

  val regions = 20

  def rdd(): RDD[(ImmutableBytesWritable, KeyValue)] = {
    val conf = new SparkConf().setAppName("Twitter-" + System.currentTimeMillis())
    val sc = new SparkContext(conf)
    val origin = sc.textFile(hadoop_file).persist(StorageLevel.MEMORY_ONLY_2)
    val json = origin.map(line => JSON.parseObject(line))
    json.map(json => {
      val timestamp: Long = json.getLong("timestamp_ms")
      val text = json.getOrDefault("text", "").toString
      val id = json.getLong("id")
      val prefix = (97 + id % regions).asInstanceOf[Char]

      val rowKey = prefix + id.toString
      val key = new ImmutableBytesWritable(Bytes.toBytes(rowKey))
      val value = new KeyValue(Bytes.toBytes(rowKey),
        Bytes.toBytes(columnFamily),
        Bytes.toBytes(columnQualify),
        timestamp,
        Bytes.toBytes(text))
      (key, value)
    })
  }

  def split(): Array[Array[Byte]] = {
    val splitSet = new Array[Array[Byte]](regions)
    val set = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
    for (i <- Range(0, regions)) {
      // From b.
      set.add(Bytes.toBytes((98 + i).asInstanceOf[Char] + "0000000000000000000"))
    }
    val itr = splitSet.iterator
    while (itr.hasNext) splitSet :+ itr.next()
    splitSet
  }

  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    Twitter.bulkLoad(true)
  }
}
