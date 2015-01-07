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

import java.util
import java.util.Comparator

trait Row {

  val str: String
}

trait TsvRow extends Row {

  def apply(row: Int): String

  def columns(from: Int, to: Int): Array[String]

  val columnCount: Int

  override def toString(): String = "TsvRow{" + str + "}"
}

class TsvRowArr(val columns: IndexedSeq[String],
                val knownStr: Option[String] = None // in some cases we know whole string of row, so lazy 'str' is cheaper
                 )(
                 implicit val keyColumns: Int,
                 implicit val columnDelimiter: Char
                 ) extends TsvRow {

  def apply(row: Int): String = columns(row)

  override lazy val str: String = knownStr match {
    case Some(str) => str
    case None => columns.mkString(columnDelimiter + "")
  }

  lazy val columnCount: Int = columns.size

  override def columns(from: Int, to: Int): Array[String] = util.Arrays.copyOfRange(columns.toArray, from, to)
}

class TsvRowImpl(val str: String
                  )(
                  implicit val keyColumns: Int,
                  implicit val columnDelimiter: Char
                  ) extends TsvRow {

  def apply(row: Int): String = columnArr(row)

  lazy val columnArr: Array[String] = str.split(columnDelimiter)

  lazy val columnCount: Int = columnArr.size

  override def columns(from: Int, to: Int): Array[String] = util.Arrays.copyOfRange(columnArr, from, to)
}

trait RowFactory[R] {

  def apply(str: String): R

  def apply(bytes: Array[Byte]): R = apply(new String(bytes))
}

class TsvRowFactory(implicit keyColumns: Int, columnDelimiter: Char)
  extends RowFactory[TsvRow] {

  // Is delimiter some ascii character < 0x80 (signed char > 0) ?
  val isAsciiDelimiter: Boolean = ((columnDelimiter & 0xff00) == 0) && (columnDelimiter.toByte > 0)
  val byteDelimiter: Byte = columnDelimiter.toByte
  val expectedColumns: Int = math.max(keyColumns + 1, 10)

  def fromString(str: String): TsvRow = new TsvRowImpl(str)

  /**
   * If delimiter character is a ascii character ( < 0x80), we can split row byte array
   * to chunks that represents columns. It is faster than build String from these bytes
   * and then split it by regular expression in
   * scala.collection.StringLike.split(separator: Char)
   */
  def fromBytes(bytes: Array[Byte], knownStr: Option[String] = None): TsvRow = {
    var i = 0
    var start = 0
    import scala.collection.mutable
    val list = new mutable.ArrayBuffer[String](expectedColumns)
    val len = bytes.length
    while (i < len) {
      if (bytes(i) == byteDelimiter) {
        list += new String(bytes, start, i - start)
        start = i + 1
      }
      i += 1
    }
    if (start == i)
      list += new String()
    else
      list += new String(bytes, start, i - start)
    new TsvRowArr(list, knownStr)
  }

  def apply(str: String): TsvRow = if (isAsciiDelimiter) {
    fromBytes(str.getBytes, Some(str))
  } else {
    fromString(str)
  }

  override def apply(bytes: Array[Byte]): TsvRow = if (isAsciiDelimiter) {
    fromBytes(bytes)
  } else {
    fromString(new String(bytes))
  }

  def apply(str: Array[String]): TsvRow = new TsvRowArr(str)
}

class TsvRowComparator(implicit val keyColumns: Int)
  extends Comparator[TsvRow] {

  def check(row: TsvRow) = if (row.columnCount < keyColumns)
    sys.error("To few columns in row")

  override def compare(row1: TsvRow, row2: TsvRow): Int = {
    check(row1)
    check(row2)

    // for (column <- (0 to keyColumns - 1)) {
    // optimized:
    var column = 0
    while (column < keyColumns) {
      val res = row1(column).compare(row2(column))
      if (res != 0)
        return res
      column += 1
    }
    return 0
  }
}