package cn.edu.thssdb.schema;

import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row> {
  ReentrantReadWriteLock lock;
  private String databaseName;
  public String tableName;
  public ArrayList<Column> columns;
  public BPlusTree<Entry, Row> index;
  private int primaryIndex;

  public Table(String databaseName, String tableName, Column[] columns) {
    // TODO
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>();
    this.columns.addAll(Arrays.asList(columns));
  }

  private void recover() {
    // TODO
    try {
      lock.writeLock().lock();
      ArrayList<Row> rows = deserialize();
      if(rows!=null){
        for (Row row: rows) {
          index.put(row.getEntries().get(primaryIndex), row);
        }
      }
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  public void insert(Row row) throws Exception {
    // TODO
    Entry primary_key = row.getEntries().get(primaryIndex);
    boolean has_exist;
    try {
      lock.readLock().lock();
      has_exist = index.contains(primary_key);
    }
    finally {
      lock.readLock().unlock();
    }
    if(!has_exist){
      try {
        lock.writeLock().lock();
        index.put(primary_key, row);
      }
      finally {
        lock.writeLock().unlock();
      }
    }
    else{
      throw new Exception("Row has existed!");
    }

  }

  public void delete(Row row) throws Exception {
    // TODO
    Entry primary_key = row.getEntries().get(primaryIndex);
    boolean has_exist;
    try {
      lock.readLock().lock();
      has_exist = index.contains(primary_key);
    }
    finally {
      lock.readLock().unlock();
    }
    if(has_exist){
      try {
        lock.writeLock().lock();
        Entry entry = row.getEntries().get(primaryIndex);
        index.remove(entry);
      }
      finally {
        lock.writeLock().unlock();
      }
    }
    else{
      throw new Exception("Row has not existed!");
    }
  }

  public void update(Row row) throws Exception {
    // TODO
    Entry primary_key = row.getEntries().get(primaryIndex);
    boolean has_exist;
    try {
      lock.readLock().lock();
      has_exist = index.contains(primary_key);
    }
    finally {
      lock.readLock().unlock();
    }
    if(has_exist){
      try {
        lock.writeLock().lock();
        Entry entry = row.getEntries().get(primaryIndex);
        index.update(entry, row);
      }
      finally {
        lock.writeLock().unlock();
      }
    }
    else{
      throw new Exception("Row has not existed!");
    }
  }

  private void serialize() {
    // TODO
  }

  private ArrayList<Row> deserialize() {
    // TODO
    return null;
  }

  private class TableIterator implements Iterator<Row> {
    private Iterator<Pair<Entry, Row>> iterator;

    TableIterator(Table table) {
      this.iterator = table.index.iterator();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public Row next() {
      return iterator.next().right;
    }
  }

  @Override
  public Iterator<Row> iterator() {
    return new TableIterator(this);
  }
}
