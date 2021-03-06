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

import com.alibaba.fastjson.JSON
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2
import org.apache.hadoop.hbase.tool.LoadIncrementalHFiles
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Twitter {

  val tableName: String = PropertyProvider.getString("hbase.bulkload.tablename")
  val columnFamily: String = PropertyProvider.getString("hbase.bulkload.columnfamily")
  val columnQualify: String = PropertyProvider.getString("hbase.bulkload.columnqualify")
  val hfile: String = PropertyProvider.getString("hbase.bulkload.hfile")
  val hadoop_dir: String = PropertyProvider.getString("hbase.bulkload.hadoopdir")
  val hadoop_file: String = PropertyProvider.getString("hbase.bulkload.hadoopfile")
  val regions = 20

  def rdd(): RDD[(ImmutableBytesWritable, KeyValue)] = {
    val conf = new SparkConf().
      setAppName("Twitter-" + System.currentTimeMillis())
    val sc = new SparkContext(conf)
    val origin = sc.textFile(hadoop_dir + hadoop_file).
      persist(StorageLevel.MEMORY_AND_DISK)
    origin.map(line => {
        val json = JSON.parseObject(line)
        if (json.containsKey("created_at")) {
          val timestamp: Long = json.getString("timestamp_ms").toLong
          val id = json.getJSONObject("user").getLong("id")
          val prefix = (97 + line.hashCode % regions).asInstanceOf[Char]
          val rowKey = prefix + id.toString
          (rowKey, (line, timestamp))
        } else {
          val timestamp: Long = json.getJSONObject("delete").
            getString("timestamp_ms").toLong
          val id = json.getJSONObject("delete").
            getJSONObject("status").getLong("user_id")
          val prefix = (97 + id % regions).asInstanceOf[Char]
          val rowKey = prefix + id.toString
          (rowKey, (line, timestamp))
        }
      }).map(x => (x._1 + x._2._2.toString, x._2)).sortByKey(ascending = true).
      map(record => {
      val key = new ImmutableBytesWritable(Bytes.toBytes(record._1))
      val value = new KeyValue(Bytes.toBytes(record._1),
        Bytes.toBytes(columnFamily),
        Bytes.toBytes(columnQualify),
        record._2._2,
        Bytes.toBytes(record._2._1))
      (key, value)
    })
  }

  def split(n: Int): Array[Array[Byte]] = {
    val split = new Array[Array[Byte]](n)
    for (i <- Range(0, n)) {
      // From b.
      val str = (98 + i).asInstanceOf[Char] + "00000000000000000000000"
      split(i) = Bytes.toBytes(str)
    }
    split
  }

  def bulkLoad(checkHTable: Boolean = false): Unit = {
    val hc = HBaseConfiguration.create
    hc.set("hbase.mapred.outputtable", tableName)
    hc.setLong("hbase.hregion.max.filesize", HConstants.DEFAULT_MAX_FILE_SIZE)
    hc.set("hbase.mapreduce.hfileoutputformat.table.name", tableName)
    hc.setInt(LoadIncrementalHFiles.MAX_FILES_PER_REGION_PER_FAMILY,
      1024 * 1024 * 1024)
    val con = ConnectionFactory.createConnection(hc)
    val admin = con.getAdmin
    val table = con.getTable(TableName.valueOf(tableName))
    val td = table.getDescriptor
    val job = Job.getInstance(hc)
    val rdd_ = rdd()
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    HFileOutputFormat2.configureIncrementalLoadMap(job, td)
    rdd_.saveAsNewAPIHadoopFile(hfile,
      classOf[ImmutableBytesWritable], classOf[KeyValue],
      classOf[HFileOutputFormat2], hc)
    val bulkLoader = new LoadIncrementalHFiles(hc)
    val locator = con.getRegionLocator(TableName.valueOf(tableName))
    bulkLoader.doBulkLoad(new Path(hfile), admin, table, locator)
  }

  def main(args: Array[String]): Unit = {
    // scalastyle:off println
    println(s"Clean hfiles >> ${HBaseCommon.cleanHFiles}")
    println(s"Delete table >> ${HBaseCommon.dropDeleteTable(tableName)}")
    println(s"Crete table >> ${HBaseCommon.createTable(tableName, Array(columnFamily),
      Twitter.split(regions - 1))}")
    Twitter.bulkLoad(true)
//    HBaseCommon.dropDeleteTable("Kowalski")
  }

}
