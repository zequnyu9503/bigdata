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
package pers.yzq.timewindow.prefetcher.loader


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileInputFormat, InputFormat, InputSplit, JobConf, RecordReader, Reporter, TextInputFormat}
import org.apache.hadoop.util.ReflectionUtils
import pers.yzq.timewindow.PropertyProvider

object UsingIterator {

  val input: String = PropertyProvider.getString("prefetcher.loader.input")

  def main(args: Array[String]): Unit = {
    // scalastyle:off println

    val iterator = getIterator
    var count: Long = 0L
    while (iterator.hasNext) {
      iterator.next()
      count += 1
      println(s"Total $count elements")
    }
  }

  def getIterator: Iterator[Text] = {
    new Iterator[Text] {

      var finished = false
      var gotNext = false
      var nextValue: Text = _

      var key: LongWritable = _
      var value: Text = _

      val jobConf: JobConf = getJobConf
      val inputFormat: InputFormat[LongWritable, Text] = getInputFormat(jobConf)
      val inputSplit: InputSplit = getPartitions(0)

      val recordReader: RecordReader[LongWritable, Text] =
        inputFormat.getRecordReader(inputSplit, jobConf, Reporter.NULL)

      key = recordReader.createKey()
      value = recordReader.createValue()


      override def hasNext: Boolean = {
        if (!finished) {
          if (!gotNext) {
            nextValue = {
              finished = !recordReader.next(key, value)
              value
            }
          }
          gotNext = true
        }
        !finished
      }

      override def next(): Text = {
        if (!hasNext) {
          throw new NoSuchElementException("End of stream")
        }
        gotNext = false
        nextValue
      }
    }
  }

  def getReader: RecordReader[LongWritable, Text] = {
    val jobConf = getJobConf
    val inputFormat = getInputFormat(jobConf)
    val inputSplit = getPartitions(0)
    inputFormat.getRecordReader(inputSplit, jobConf, Reporter.NULL)
  }

  def getJobConf: JobConf = {
    val conf = new Configuration()
    conf.addResource("pers/yzq/timewindow/core-site.xml")
    conf.addResource("pers/yzq/timewindow/hdfs-site.xml")
    conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
    val newJobConf = new JobConf(conf)
    FileInputFormat.setInputPaths(newJobConf, new Path(input))
    newJobConf
  }

  def getInputFormat(jobConf: JobConf): InputFormat[LongWritable, Text] = {
    val inputFormatClass = classOf[TextInputFormat].asInstanceOf[Class[_]]
    ReflectionUtils
      .newInstance(inputFormatClass, jobConf)
      .asInstanceOf[InputFormat[LongWritable, Text]]
  }

  def getPartitions: Array[InputSplit] = {
    val jobConf = getJobConf
    val allInputSplits = getInputFormat(jobConf).getSplits(jobConf, 2)
    val array = new Array[InputSplit](allInputSplits.length)
    for (i <- array.indices) {
      array(i) = allInputSplits(i)
    }
    array
  }
}
