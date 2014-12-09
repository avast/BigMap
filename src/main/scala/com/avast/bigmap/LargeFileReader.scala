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

import java.io.{OutputStreamWriter, FileOutputStream, FileInputStream, File}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.nio.channels.FileChannel
import java.{util => ju}

object LargeFileReader {
  val RowDelimiter: Byte = '\n'

  val PageSize = Int.MaxValue
}

/**
 * Utility for dealing with large text files. It maps given file to process memory because random access
 * to memory mapped files is faster than using nio channel.
 *
 * Because nio ByteBuffers uses integers in api, we have to use "pages" with size Int.MaxValue to access
 * files greater than 2 GiB
 *
 * @param file
 */
class LargeFileReader(file: File) {

  protected var _length: Long = -1

  lazy val length: Long = {
    buffers.length // create buffers
    _length
  }

  lazy val buffers: Array[MappedByteBuffer] = {
    val is: FileInputStream = new FileInputStream(file)
    val channel = is.getChannel
    val buffs = new ju.ArrayList[MappedByteBuffer]()
    try {
      var start: Long = 0
      var length: Long = 0
      var index: Long = 0
      while (start + length < channel.size) {
        if ((channel.size / LargeFileReader.PageSize) == index)
          length = (channel.size - index * LargeFileReader.PageSize)
        else
          length = LargeFileReader.PageSize

        start = index * LargeFileReader.PageSize
        buffs.add(index.asInstanceOf[Int], channel.map(FileChannel.MapMode.READ_ONLY, start, length))
        index += 1
      }
      _length = channel.size()
    } finally {
      is.close()
    }
    buffs.toArray(new Array[MappedByteBuffer](buffs.size()))
  }

  /**
   * Get single byte from file
   *
   * @param bytePosition
   * @return
   */
  def apply(bytePosition: Long): Byte = {
    val page: Int = (bytePosition / LargeFileReader.PageSize).toInt
    val index: Int = (bytePosition % LargeFileReader.PageSize).toInt
    buffers(page).get(index)
  }

  /**
   * Get bytes from file area.
   *
   * When area cross page boundary, bytes are copied into heap. Otherwise is returned slice of memory mapped buffer.
   *
   * @param from start offset of file area
   * @param length length of file area. It can be greater than Int.MaxValue
   * @return buffer with area bytes
   */
  def apply(from: Long, length: Long): ByteBuffer = {
    if (length > Int.MaxValue)
      sys.error(s"Cant allocate buffer for ${length} bytes.")

    val fromPage: Int = (from / LargeFileReader.PageSize).asInstanceOf[Int]
    val fromIndex: Int = (from % LargeFileReader.PageSize).asInstanceOf[Int]

    val to = from + length
    val toPage: Int = (to / LargeFileReader.PageSize).asInstanceOf[Int]
    val toIndex: Int = (to % LargeFileReader.PageSize).asInstanceOf[Int]

    if (fromPage == toPage) {
      // same page, just slice buffer
      val sliced = buffers(fromPage).slice()
      sliced.position(fromIndex)
      sliced.limit(toIndex)
      sliced
    } else {
      val res = new Array[Byte](length.toInt)

      val buff = buffers(fromPage).slice()
      buff.position(fromIndex)
      buff.get(res, 0, LargeFileReader.PageSize - fromIndex)

      val end = buffers(toPage).slice()
      end.position(0)
      end.get(res, LargeFileReader.PageSize - fromIndex, toIndex)

      ByteBuffer.wrap(res)
    }
  }

  /**
   * Read file area as String.
   *
   * @param from inclusive
   * @param to exclusive
   * @return string content from file area
   */
  def string(from: Long, to: Long): String = {
    val buff = apply(from, to - from)
    if (buff.hasArray) {
      new String(buff.array(), buff.position(), buff.remaining())
    } else {
      val arr = new Array[Byte](buff.remaining())
      buff.get(arr)
      new String(arr)
    }
  }

  /**
   * Search first byte occurrence before start position (exclusive).
   *
   * @param start exclusive
   * @param searched
   * @return position of searched byte or -1 if byte dont occurs before start
   */
  def searchPrev(start: Long, searched: Byte): Long = {
    var off = start - 1
    while (off >= 0) {
      val ch = this(off)
      if (ch == searched)
        return off

      off -= 1
    }
    -1
  }

  /**
   * Search first byte occurrence after start position (inclusive).
   *
   * @param start inclusive
   * @param searched
   * @return position of searched byte or file length if byte dont occurs after start
   */
  def searchNext(start: Long, searched: Byte): Long = {
    var off = start
    while (off < length) {
      val ch = this(off)
      if (ch == searched)
        return off

      off += 1
    }
    length
  }

  /**
   * Create iterator for lines in file area
   *
   * @param fromOff inclusive
   * @param toOff exclusive
   * @return iterator for rows
   */
  def getLines(fromOff: Long, toOff: Long): Iterator[String] = new Iterator[String] {

    var pos: Long = fromOff

    override def hasNext: Boolean = pos < toOff

    override def next(): String = {
      val end = Math.min(toOff, searchNext(pos, LargeFileReader.RowDelimiter))
      val str = string(pos, end)
      pos = end + 1
      str
    }
  }

  /**
   * Create iterator for lines in file
   *
   * @return iterator for rows
   */
  def getLines(): Iterator[String] = getLines(0, length)
}

/**
 * Test copy text files using LargeFileReader
 */
object TestCopyLargeFile {

  def main(args: Array[String]): Unit = {
    val inFile = args(0)
    val outFile = args(1)
    val f = new LargeFileReader(new File(inFile))
    val out = new OutputStreamWriter(new FileOutputStream(new File(outFile)))
    var i: Long = 0
    val it = f.getLines()
    for (line <- it) {
      if (i % 100000 == 0)
        println(i + ". " + it + ": " + line)
      i += 1
      out.write(line)
      out.write('\n')
    }
    out.close()
  }
}
