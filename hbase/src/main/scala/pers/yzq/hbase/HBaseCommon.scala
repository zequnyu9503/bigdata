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

import java.io.IOException
import java.net.{URI, URISyntaxException}
import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptor, ColumnFamilyDescriptorBuilder, ConnectionFactory, HBaseAdmin, TableDescriptorBuilder}
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.internal.Logging

object HBaseCommon extends Logging{
  val path: String = PropertyProvider.getString("hdfs.home")
  val user: String = PropertyProvider.getString("hdfs.user")
  val hfiles: String = PropertyProvider.getString("hbase.bulkload.hfile")

  def createTable(tableName: String,
                  families: Array[String],
                  splits: Array[Array[Byte]]): Boolean = {
    assert(!tableName.eq(null))
    assert(!families.eq(null))
    assert(!splits.eq(null) && splits.nonEmpty)
    // scalastyle:off println
    try {
      val hBaseConfiguration = HBaseConfiguration.create()
      hBaseConfiguration.set("hbase.zookeeper.quorum",
        "centos3,centos4,centos5,centos11,centos12,centos13")
      hBaseConfiguration.set("hbase.master", "centos3:6000")
      val connection = ConnectionFactory.createConnection(hBaseConfiguration)
      val admin = connection.getAdmin
      val tn = TableName.valueOf(tableName)
      val tdb = TableDescriptorBuilder.newBuilder(tn)
      val cfdbs = new util.HashSet[ColumnFamilyDescriptor](families.length)
      val familyIterator = families.iterator
      while (familyIterator.hasNext) {
        val family = familyIterator.next
        val cfdb = ColumnFamilyDescriptorBuilder
          .newBuilder(Bytes.toBytes(family))
          .setBlockCacheEnabled(true)
          .setDataBlockEncoding(DataBlockEncoding.NONE)
        cfdbs.add(cfdb.build)
      }
        admin.createTable(tdb.setColumnFamilies(cfdbs).build, splits)
        admin.close()
        true
    } catch {
      case e: IOException =>
        e.printStackTrace()
        false
    }
  }

  def createTable(tableName: String, families: String*): Boolean = {
    val hBaseConfiguration = HBaseConfiguration.create()
    hBaseConfiguration.set("hbase.zookeeper.quorum",
      "centos3,centos4,centos5,centos11,centos12,centos13")
    hBaseConfiguration.set("hbase.master", "centos3:6000")
    val connection = ConnectionFactory.createConnection(hBaseConfiguration)
    val admin = connection.getAdmin
    val htd = new HTableDescriptor(TableName.valueOf(tableName))
    for (family <- families) htd.addFamily(new HColumnDescriptor(family))
    try{
      admin.createTable(htd)
      true
    } catch {
      case e: IOException =>
        false
    }
  }

  @deprecated
  def createTable(tableName: String, families: Array[String]): Boolean = {
    try {
      val hBaseConfiguration = HBaseConfiguration.create()
      hBaseConfiguration.set("hbase.zookeeper.quorum",
        "centos3,centos4,centos5,centos11,centos12,centos13")
      hBaseConfiguration.set("hbase.master", "centos3:6000")
      val connection = ConnectionFactory.createConnection(hBaseConfiguration)
      val admin = connection.getAdmin
      val htd = new HTableDescriptor(TableName.valueOf(tableName))
      families.foreach(e => htd.addFamily(new HColumnDescriptor(Bytes.toBytes(e))))
      admin.createTable(htd)
      logInfo(s"Create htable $tableName.")
      admin.close()
      true
    } catch {
      case e: IOException =>
        e.printStackTrace()
        false
    }
  }

  def dropDeleteTable(tableName: String): Boolean = {
    try {
      val hBaseConfiguration = HBaseConfiguration.create()
      hBaseConfiguration.set("hbase.zookeeper.quorum",
        "centos3,centos4,centos5,centos11,centos12,centos13")
      hBaseConfiguration.set("hbase.master", "centos3:6000")
      val connection = ConnectionFactory.createConnection(hBaseConfiguration)
      val admin = connection.getAdmin
      val tn = TableName.valueOf(tableName)
      if (admin.tableExists(tn)) {
        logInfo(s"Disable and delete htable $tableName.")
        if (!admin.isTableDisabled(tn)) admin.disableTable(tn)
        admin.deleteTable(tn)
        admin.close()
      }
      true
    } catch {
      case e: IOException => e.printStackTrace()
        false
    }
  }

  def cleanHFiles: Boolean = {
    try {
      val configuration = new Configuration
      val fileSystem = FileSystem.get(new URI(path), configuration, user)
      if (fileSystem.exists(new Path(hfiles))) {
        logInfo("HFiles exist then remove them.")
        return fileSystem.delete(new Path(hfiles), true)
      } else return true
    } catch {
      case e: IOException => e.printStackTrace()
      case e: URISyntaxException => e.printStackTrace()
      case e: InterruptedException => e.printStackTrace()
    }
    false
  }
  
}
