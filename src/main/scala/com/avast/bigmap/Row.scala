/*
 *  Copyright 2013 Lukas Karas, Avast a.s. <karas@avast.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.avast.bigmap

import java.util.Comparator

trait Row {
  val str: String

}

trait TsvRow extends Row {
  val columns: Array[String]

  def apply(row: Int): String = columns(row)

  override def toString(): String = s"TsvRow{${str}}"
}

class TsvRowArr(val columns: Array[String]
                 )(
                 implicit val keyColumns: Int,
                 implicit val columnDelimiter: Char
                 ) extends TsvRow {

  override lazy val str: String = columns.mkString(columnDelimiter + "")
}

class TsvRowImpl(val str: String
                  )(
                  implicit val keyColumns: Int,
                  implicit val columnDelimiter: Char
                  ) extends TsvRow {

  lazy val columns = str.split(columnDelimiter)
}

class TsvRowFactory(implicit keyColumns: Int, columnDelimiter: Char) {

  def apply(str: String): TsvRow = new TsvRowImpl(str)

  def apply(str: Array[String]): TsvRow = new TsvRowArr(str)
}

class TsvRowComparator(implicit val keyColumns: Int)
  extends Comparator[TsvRow] {

  def check(row: TsvRow) = if (row.columns.length < keyColumns)
    sys.error("To few columns in row")

  override def compare(row1: TsvRow, row2: TsvRow): Int = {
    check(row1)
    check(row2)

    for (column <- (0 to keyColumns - 1)) {
      val res = row1(column).compare(row2(column))
      if (res != 0)
        return res
    }
    return 0
  }
}