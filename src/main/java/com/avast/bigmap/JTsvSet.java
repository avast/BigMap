package com.avast.bigmap;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

public class JTsvSet implements Set<String[]> {

  protected final JTsvMap map;

  public JTsvSet(JTsvMap map) {
    this.map = map;
  }

  public JTsvSet(File sortedFile) {
    this(sortedFile, JTsvMap.DEFAULT_KEY_COLUMNS, JTsvMap.DEFAULT_COLUMN_DELIMITER);
  }

  public JTsvSet(File sortedFile, int keyColumns, char columnDelimiter) {
    this(new JTsvMap(sortedFile, keyColumns, columnDelimiter));
  }

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return map.containsKey(o);
  }

  @Override
  public Iterator<String[]> iterator() {
    return map.keyIterator();
  }

  @Override
  public Object[] toArray() {
    throw new UnsupportedOperationException("JTsvSet don't implement toArray method.");
  }

  @Override
  public <T> T[] toArray(T[] a) {
    throw new UnsupportedOperationException("JTsvSet don't implement toArray method.");
  }

  @Override
  public boolean add(String[] strings) {
    throw new UnsupportedOperationException("JTsvSet is immutable.");
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException("JTsvSet is immutable.");
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    throw new UnsupportedOperationException("JTsvSet don't implement containsAll method.");
  }

  @Override
  public boolean addAll(Collection<? extends String[]> c) {
    throw new UnsupportedOperationException("JTsvSet is immutable.");
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException("JTsvSet is immutable.");
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException("JTsvSet is immutable.");
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException("JTsvSet is immutable.");
  }
}
