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


import scala.io.Source


// scalastyle:off println
object Sougou {

  /*
  数据格式:
  访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
  其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对应同一个用户ID
   */

  def main(args: Array[String]): Unit = {
    val path = "F:\\BaiduNetdiskDownload\\sougou\\access_log.20080601.decode.filter"
    val itr = Source.fromFile(path, "gbk").getLines()
    val len = 200
    var i = 0
    while (itr.hasNext && i < len) {
      println(itr.next())
      i += 1
    }
  }
}