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

import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object DataCreater extends BulkLoad {

  tableName = PropertyProvider.getString("hbase.bulkload.tablename")
  columnFamily = PropertyProvider.getString("hbase.bulkload.columnfamily")
  columnQualify = PropertyProvider.getString("hbase.bulkload.columnqualify")
  hfile = PropertyProvider.getString("hbase.bulkload.hfile")
  hadoop_file = PropertyProvider.getString("hbase.bulkload.hadoopfile")

  def main(args: Array[String]): Unit = {

  }

  override def rdd(): RDD[(ImmutableBytesWritable, KeyValue)] = {
    val conf = new SparkConf().setAppName("Data Create BulkLoad-" + System.currentTimeMillis())
    val sc = new SparkContext(conf)
    sc.textFile(hadoop_file).persist(StorageLevel.MEMORY_AND_DISK)
      .map(e => e.split("-"))
      .map(e => (e(0).toLong, e (1).toLong))
      .map(e => {
        val timestamp = String.format(e._2.toString, "%0" + "10d")
        val header = (97 + e._2 %10).asInstanceOf[Char]
        val rowKey = header + timestamp
        val key = new ImmutableBytesWritable(Bytes.toBytes(rowKey))
        val value = new KeyValue(Bytes.toBytes(rowKey),
          Bytes.toBytes(columnFamily),
          Bytes.toBytes(columnQualify),
          e._2,
          Bytes.toBytes(e._1))
        (key, value)
      })
  }

  override def split(): Array[Array[Byte]] = {
    // 255081332.
    val splitSet = new Array[Array[Byte]](20)
    val set = new util.TreeSet[Array[Byte]](Bytes.BYTES_COMPARATOR)
    for (i <- Range(0, 20)) {
      // From b.
      set.add(Bytes.toBytes((98 + i).asInstanceOf[Char] + "0000000000"))
    }
    val itr = splitSet.iterator
    while (itr.hasNext) splitSet :+ itr.next()
    splitSet
  }
}
