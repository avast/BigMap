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
 * Original source:
 * http://stackoverflow.com/questions/736556/binary-search-in-a-sorted-memory-mapped-file-in-java
 */
package com.avast.bigmap

import java.io.File
import java.util.Comparator

import scala.annotation.tailrec

class BinarySearch[R <: Row](sortedTextFile: LargeFileReader,
                             sequenceSearchTopLimit: Long = 0
                              )(
                              implicit rowFactory: String => R,
                              rowComparator: Comparator[R]
                              ) {

  def searchSequence(row: R, from: Long, to: Long): Option[R] = sortedTextFile
    .getLines(from, to)
    .map(rowFactory(_))
    .filter(rowComparator.compare(row, _) == 0)
    .toStream
    .headOption

  @tailrec
  final def search(row: R, from: Long, to: Long, depth: Int = 0): Option[R] = {
    if (to - from <= 0)
      return None
    if (to - from < sequenceSearchTopLimit)
      return searchSequence(row, from, to)

    val half = from + ((to - from) / 2)
    val prevDelim = sortedTextFile.searchPrev(half, LargeFileReader.RowDelimiter)
    val nextDelim = sortedTextFile.searchNext(half, LargeFileReader.RowDelimiter)
    val candidate = rowFactory(sortedTextFile.string(prevDelim + 1, nextDelim))

    val cmp = rowComparator.compare(row, candidate)
    if (cmp < 0) {
      search(row, from, prevDelim, depth + 1)
    } else if (cmp > 0) {
      search(row, nextDelim + 1, to, depth + 1)
    } else {
      Some(candidate)
    }
  }

  def search(row: R): Option[R] = {
    search(row, 0, sortedTextFile.length)
  }
}

object BinarySearchTest {

  val columnDelimiter = '\t'
  val keyColumns = 1

  def main(args: Array[String]): Unit = {
    val sortedFile = args(0)
    val sample = args(1)

    val row = new TsvRowFactory()(keyColumns, columnDelimiter)
    val f = new LargeFileReader(new File(sortedFile))
    val s = new BinarySearch[TsvRow](sortedTextFile = f, sequenceSearchTopLimit = 0)(
      row.apply,
      new TsvRowComparator()(keyColumns)
    )

    val testFile = new LargeFileReader(new File(sample))

    while (true) {
      val start = System.currentTimeMillis()
      val foundAll = testFile
        .getLines()
        .map(row(_))
        .map(s.search(_))
        .forall(_.isDefined)

      if (!foundAll)
        sys.error("some row missing!")

      println("duration " + (System.currentTimeMillis() - start) + " ms")
    }
  }
}
