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

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSON

object TimestampSuite {
  // scalastyle:off println

  def long2Calendar(stamp: Long): Calendar = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(stamp)
    cal
  }

  def main(args: Array[String]): Unit = {
//    val cal = TimestampSuite.long2Calendar(1554146018666L)
//    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    println(format.format(cal.getTime))
   }
}
