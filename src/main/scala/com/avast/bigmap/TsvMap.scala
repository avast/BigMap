/**
 *
 * Copyright 2014 Lukas Karas, Avast a.s. <karas@avast.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.avast.bigmap

import java.io.File

class TsvMap(sortedTextFile: File)(
  implicit val keyColumns: Int = 1,
  implicit val columnDelimiter: Char = '\t'
  ) extends Map[Array[String], Array[String]] {

  val rowFactory = new TsvRowFactory()

  lazy val fileReader = new LargeFileReader(sortedTextFile)
  lazy val bSearch = new BinarySearch[TsvRow](fileReader)(
    rowFactory,
    new TsvRowComparator()
  )

  override def +[B1 >: Array[String]](kv: (Array[String], B1)): Map[Array[String], B1] = sys.error("Extending map is not supported")

  def value(row: TsvRow): Array[String] = row.columns(keyColumns, row.columnCount)

  def key(row: TsvRow): Array[String] = row.columns(0, keyColumns)

  override def get(key: Array[String]): Option[Array[String]] = bSearch
    .search(rowFactory(key))
    .map(value(_))

  override def iterator: Iterator[(Array[String], Array[String])] = fileReader
    .getLines()
    .map[(Array[String], Array[String])](line => {

    val row = rowFactory(line)
    (key(row), value(row))
  })

  override def -(key: Array[String]): Map[Array[String], Array[String]] = sys.error("Removing from map is not supported")
}

object TsvMapTest {

  val columnDelimiter = '\t'
  val keyColumns = 1

  def main(args: Array[String]): Unit = {
    val sortedFile = args(0)
    val key = args.tail

    val map = new TsvMap(new File(sortedFile))
    val v = map.get(key)
    println(key.mkString("(", ", ", ")") + " => " + v.map(arr => arr.mkString("(", ", ", ")")))
  }
}
