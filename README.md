BigMap
======

In some data processing tasks we need to use huge maps or sets that are bigger
than available JVM heap space or they are loading too slow to standard Java or Scala Maps.

We use TSV format (text file with tab separated columns) for persist this kind of Maps or Sets.
Some columns are used as a key and rest of columns as a value.

Idea of this library is simple. We can prepare these maps once (sort by key),
store it to file and then use it as memory mapped file.
Searching key in sorted file has log(n) complexity.

PROS:

 * Makes possible to use huge data sets.
 * Initilization is fast. Memory mapped File can be loaded lazy by OS.
 * If more processes uses the same memory mapped file, it exists in memory just once (on Linux).
 
CONS:

 * Usage is slower than with standard Maps in memory.


Sorting of huge TSV files
=========================

Part of this this project is a tool for sorting huge TSV files.
Sorting have is processed in two steps:

 * division input file(s) to row chunks (these chunks are sorted in heap) and write chunks to disk
 * merge temporary files to output file

	java -cp ${CLASSPATH} com.avast.bigmap.TsvRowSorter \
	--input intputDirectoryOrFile \
	--output sortedOutputFile \
	--key-columns 1 

Usage
=====

	val map = new TsvMap(new File(sortedTsvFile))
	val key:Array[String] = Array("somekey")
	val value:Option[Array[String]] = map.get(key)

