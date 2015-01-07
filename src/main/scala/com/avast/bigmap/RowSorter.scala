/*
 *  Copyright 2014 Lukas Karas, Avast a.s. <karas@avast.com>
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

import java.io.{File, FileInputStream, FileOutputStream, OutputStreamWriter}
import java.util
import java.util.Comparator
import java.util.concurrent.LinkedBlockingQueue

import org.apache.commons.io.FileUtils
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.Iterator
import scala.io.Source
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

/**
 * Util class for sorting huge text files (bigger than heap size) by rows.
 * Sorting have to be processed by two steps:
 *
 * - division input file(s) to row chunks (these chunks are sorted in heap)
 * and write chunks to disk
 *
 * - merge temporary files to output file
 *
 * @param rowFactory
 * @param rowComparator
 * @param tempFileSuffix
 *
 * @tparam R
 */
class RowSorter[R <: Row](rowFactory: RowFactory[R],
                          rowComparator: Comparator[R],
                          tempFileSuffix: String = ".tmp") {

  val rowDelimiter: Char = '\n'
  val logger: Logger = LoggerFactory.getLogger("RowSorter")

  /**
   * Splits input files to chunks of lines (max size of chunk is memorySortRows),
   * sorts these chunks in memory (by util.Arrays.sort) and writes sorted chunks
   * to files in temp directory.
   *
   * @param inputFiles
   * @param memorySortRows
   * @return Sequence of files with sorted line chunks.
   */
  def splitToSortedChunks(inputFiles: Seq[File], memorySortRows: Int): Seq[File] = {

    def sortRows(rows: Array[Row], from: Int, to: Int): File = {
      val chunkOutputFile = tmpFile()
      util.Arrays.sort(rows, from, to, rowComparator.asInstanceOf[Comparator[Row]])
      val out = new OutputStreamWriter(new FileOutputStream(chunkOutputFile))

      val sliced = if (to == rows.length) rows else rows.slice(from, to)
      for (row <- sliced) {
        out.write(row.str)
        out.write(rowDelimiter)
      }
      out.close()
      logger.debug("Sort " + (to - from) + " lines to temp file " + chunkOutputFile + "")
      chunkOutputFile
    }


    val chunkSeq = inputFiles.par.map(f => {

      val state = Source.fromInputStream(new FileInputStream(f))
        .getLines()
        .foldLeft((0, new Array[Row](memorySortRows), List[File]()))((state, row) => {
        val pos = state._1
        val rowArr = state._2
        val sortedChunks = state._3
        rowArr(pos) = rowFactory(row)
        if (pos == rowArr.length - 1) {
          // rowArr buffer is full, sort it to temp file
          (0, new Array[Row](memorySortRows), sortedChunks :+ sortRows(rowArr, 0, rowArr.length))
        } else {
          (pos + 1, rowArr, sortedChunks)
        }
      })

      // sort last chunk
      val pos = state._1
      val rowArr = state._2
      val sortedChunks = state._3
      if (pos > 0) {
        sortedChunks :+ sortRows(rowArr, 0, pos)
      } else {
        sortedChunks
      }

    })

    chunkSeq.seq.flatten
  }

  def tmpFile(prefix: String = "sortedChunk"): File = File.createTempFile(prefix, tempFileSuffix)

  /**
   * Merge synchronously two sorted text files into sorted output.
   *
   * This method do not remove input files.
   *
   * @param af
   * @param bf
   * @return File in temp directory with merged content.
   */
  def mergeSortedFiles(af: File, bf: File): File = {
    val as = Source.fromFile(af)
    val bs = Source.fromFile(bf)
    val outFile = tmpFile("merged")
    val out = new OutputStreamWriter(new FileOutputStream(outFile))
    try {
      def writeRow(row: R): Unit = {
        out.write(row.str)
        out.write(rowDelimiter)
      }

      val aIt = as.getLines().map(rowFactory(_))
      val bIt = bs.getLines().map(rowFactory(_))
      var aRow: Option[R] = None
      var bRow: Option[R] = None

      while (aIt.hasNext || bIt.hasNext || aRow.isDefined || bRow.isDefined) {
        aRow = aRow match {
          case Some(row) => Some(row)
          case None => if (aIt.hasNext) Some(aIt.next()) else None
        }
        bRow = bRow match {
          case Some(row) => Some(row)
          case None => if (bIt.hasNext) Some(bIt.next()) else None
        }


        (aRow, bRow) match {
          case (Some(a), Some(b)) => {
            val cmp = rowComparator.compare(a, b) // compare A, B; write smaller
            if (cmp <= 0) {
              writeRow(a)
              aRow = None
            } else {
              writeRow(b)
              bRow = None
            }
          }
          case (Some(a), None) => {
            // B ends, write remaining A
            writeRow(a)
            aRow = None
            for (row <- aIt) writeRow(row)
          }
          case (None, Some(b)) => {
            // A ends, write remaining B
            writeRow(b)
            bRow = None
            for (row <- bIt) writeRow(row)
          }
          case (None, None) => sys.error("Should not happen!")
        }
      }
      outFile
    } finally {
      as.close()
      bs.close()
      out.close()
    }
  }

  /**
   * Merge sorted input files in parallel to sorted output.
   *
   * This method will DELETE input files!
   *
   * This method will do log(files.size) merge operations.
   *
   * @param files
   * @param outputFile
   */
  def mergeSort(files: Seq[File], outputFile: File): Unit = {

    class FileIt extends Iterator[File] {
      val queue = {
        val q = new LinkedBlockingQueue[File](files.size)
        import scala.collection.JavaConversions._
        q.addAll(files)
        q
      }
      @volatile
      var remainingTasks = 0

      override def hasNext: Boolean = queue.synchronized {
        remainingTasks > 0 || (!queue.isEmpty)
      }

      override def next(): File = queue.take()

      def push(f: Future[File]): Unit = queue.synchronized {
        remainingTasks += 1
        f.onComplete(t => queue.synchronized {
          queue.put(t.get)
          remainingTasks -= 1
          logger.debug("remaining " + remainingTasks + " merge tasks, " + queue.size() + " files in queue")
        })
      }
    }

    val it = new FileIt

    while (it.hasNext) {
      val a = it.next()
      if (it.hasNext) {
        val b = it.next()
        val f: Future[File] = future {
          val out = mergeSortedFiles(a, b)
          logger.debug("" + a + ", " + b + " merged to " + out + "")
          a.delete()
          b.delete()
          out
        }
        it.push(f)

      } else {
        // last chunk
        val src = a
        if (src.renameTo(outputFile)) {
          logger.debug("Last chunk, rename " + files(0) + " to " + outputFile + "")
        } else {
          FileUtils.copyFile(src, outputFile)
          src.delete()
          logger.debug("Last chunk, copy " + files(0) + " to " + outputFile + "")
        }
      }
    }
  }

}

object TsvRowSorter extends App {
  val logger = LoggerFactory.getLogger(this.getClass)

  val columnDelimiter: Char = '\t'

  case class Args(input: Option[String] = None,
                  outputFile: Option[String] = None,
                  keyColumns: Int = 1,
                  memorySortLimit: Int = 1000 * 1000)

  val argParser = new scopt.OptionParser[Args]("tsv-sorter") {
    head("TSV Sorter", "1.0")
    opt[String]("input")
      .action((v, c) => c.copy(input = Some(v)))
      .text("Input TSV file or directory with more TSV files.")
    opt[String]("output")
      .action((v, c) => c.copy(outputFile = Some(v)))
      .text("Sorted output file.")
    opt[Int]("key-columns")
      .action((v, c) => c.copy(keyColumns = v))
      .text("Number of key columns (default " + Args().keyColumns + ").")
    opt[Int]("memory-sort-limit")
      .action((v, c) => c.copy(memorySortLimit = v))
      .text("Number of key sorted in memory (default " + Args().memorySortLimit + ").")
  }


  argParser.parse(args, Args()) match {
    case Some(Args(input, output, keyColumns, memorySortLimit)) => {
      if (keyColumns <= 0)
        sys.error("Key columns have to be more than zero.")
      if (keyColumns <= 0)
        sys.error("Memory sort limit have to be greater than zero.")

      val inputFile = new File(input match {
        case Some(input) => input
        case None => sys.error("No input specified.")
      })
      val inputFiles: Seq[File] = if (inputFile.isFile) Seq(inputFile) else inputFile.listFiles()
      if (inputFiles == null)
        sys.error("Bad input.")

      val outputFile = new File(output match {
        case Some(out) => out
        case None => sys.error("No output specified.")
      })
      if (outputFile.exists())
        sys.error("Output already exists.")

      val sorter = new RowSorter[TsvRow](
        new TsvRowFactory()(keyColumns, columnDelimiter),
        new TsvRowComparator()(keyColumns),
        ".tsv")

      val start0 = System.currentTimeMillis()
      val sortedChunks = sorter.splitToSortedChunks(inputFiles, memorySortLimit)
      logger.info("Sort chunks duration: " + (System.currentTimeMillis() - start0) + " ms")

      val start = System.currentTimeMillis()
      //mergeSort(sortedChunks, outputFile)(keyColumns, columnDelimiter)
      sorter.mergeSort(sortedChunks, outputFile)
      logger.info("Merge duration: " + (System.currentTimeMillis() - start) + " ms")
    }
    case None => sys.error("Cant parse arguments.")
  }

}

