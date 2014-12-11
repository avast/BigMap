package com.avast.bigmap;

import scala.Function0;
import scala.Option;
import scala.Tuple2;


import java.io.File;
import java.util.*;

public class JTsvMap implements Map<String[], String[]> {

  public static final int DEFAULT_KEY_COLUMNS = 1;
  public static final char DEFAULT_COLUMN_DELIMITER = '\t';

  protected final TsvMap map;

  public JTsvMap(File sortedFile) {
    this(sortedFile, DEFAULT_KEY_COLUMNS, DEFAULT_COLUMN_DELIMITER);
  }

  public JTsvMap(File sortedFile, int keyColumns, char columnDelimiter) {
    map = new TsvMap(sortedFile, keyColumns, columnDelimiter);
  }

  /**
   * This don't compute real size!
   *
   * @return
   */
  @Override
  public int size() {
    if (map.fileReader().length() > 0)
      return Integer.MAX_VALUE;
    return 0;
  }

  @Override
  public boolean isEmpty() {
    return map.fileReader().length() == 0;
  }

  @Override
  public boolean containsKey(Object key) {
    return get(key) != null;
  }

  @Override
  public boolean containsValue(Object value) {
    throw new UnsupportedOperationException("TsvMap don't implement containsValue method.");
  }

  public static <T> T getOrElse(Option<T> opt, T def) {
    if (opt.isDefined())
      return opt.get();
    else
      return def;
  }

  @Override
  public String[] get(Object keyObj) {
    if (keyObj instanceof String[])
      return getOrElse(map.get((String[]) keyObj), null);
    else
      return getOrElse(map.get(new String[]{keyObj.toString()}), null);
  }

  @Override
  public String[] put(String[] key, String[] value) {
    throw new UnsupportedOperationException("TsvMap is immutable.");
  }

  @Override
  public String[] remove(Object key) {
    throw new UnsupportedOperationException("TsvMap is immutable.");
  }

  @Override
  public void putAll(Map<? extends String[], ? extends String[]> m) {
    throw new UnsupportedOperationException("TsvMap is immutable.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("TsvMap is immutable.");
  }

  @Override
  public Set<String[]> keySet() {
    return new JTsvSet(this);
  }

  @Override
  public Collection<String[]> values() {
    throw new UnsupportedOperationException("TsvMap don't implement values method.");
  }

  @Override
  public Set<Entry<String[], String[]>> entrySet() {
    throw new UnsupportedOperationException("TsvMap don't implement entrySet method.");
  }

  public Iterator<String[]> keyIterator() {
    final scala.collection.Iterator<Tuple2<String[], String[]>> scalaIterator = map.iterator();
    return new Iterator<String[]>() {

      @Override
      public boolean hasNext() {
        return scalaIterator.hasNext();
      }

      @Override
      public String[] next() {
        return scalaIterator.next()._1();
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("TsvMap is immutable.");
      }
    };
  }

  public static void main(String[] args) {
    String sortedFile = args[0];
    String key = args[1];

    Map<String[], String[]> map = new JTsvMap(new File(sortedFile));
    String[] v = map.get(key);
    if (v == null) {
      System.out.println(key + " => null");
    } else {
      System.out.println(key + " => " + Arrays.asList(v) + "");
    }
  }
}
