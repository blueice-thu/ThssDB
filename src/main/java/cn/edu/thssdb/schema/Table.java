package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;

import java.io.*;
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
    // T O D O
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.columns = new ArrayList<>();
    this.columns.addAll(Arrays.asList(columns));
    this.index = new BPlusTree<>();
    this.lock = new ReentrantReadWriteLock();

    // init primary index
    this.primaryIndex = -1;
    int len = this.columns.size();
    for (int i = 0; i < len; i++) {
      if (this.columns.get(i).isPrimary()) {
        this.primaryIndex = i;
        break;
      }
    }
    if (this.primaryIndex == -1) {
      // no primary, set one
      this.columns.get(0).setPrimary();
      this.primaryIndex = 0;
    }
  }

  private void recover() {
    // T O D O
    try {
      lock.writeLock().lock();

      ArrayList<Row> rows = deserialize();
      if (rows != null) {
        for (Row row: rows) {
          Entry primary = row.getEntries().get(primaryIndex);
          index.put(primary, row);
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  boolean isPrimaryKeyExist(Entry primary) {
    try {
      lock.readLock().lock();
      return index.contains(primary);
    } finally {
      lock.readLock().unlock();
    }
  }

  public void insert(Row row)
    throws DuplicateKeyException {
    // T O D O
    try {
      Entry primary = row.getEntries().get(primaryIndex);
      if (isPrimaryKeyExist(primary)) {
        throw new DuplicateKeyException();
      }

      lock.writeLock().lock();
      index.put(primary, row);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void delete(Row row)
    throws KeyNotExistException {
    // T O D O
    try {
      Entry primary = row.getEntries().get(primaryIndex);
      if (!isPrimaryKeyExist(primary)) {
        throw new KeyNotExistException();
      }
      lock.writeLock().lock();
      index.remove(primary);
    } finally {
      lock.writeLock().unlock();
    }
  }

  public void update(Row oRow, Row nRow)
    throws KeyNotExistException {
    // T O D O
    try {
      Entry oPrimary = oRow.getEntries().get(primaryIndex);
      Entry nPrimary = nRow.getEntries().get(primaryIndex);
      if (!isPrimaryKeyExist(oPrimary)) {
        throw new KeyNotExistException();
      }

      lock.writeLock().lock();
      if (oPrimary.compareTo(nPrimary) == 0) {
        // same primary, update directly
        index.update(oPrimary, nRow);
      } else {
        // different primary, delete and re-insert
        delete(oRow);
        insert(nRow);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private String getBasePersistDir() {
    return Global.PERSIST_PATH + File.separator + databaseName + File.separator + "data";
  }

  private String getTablePersistDir() {
    return getBasePersistDir() + File.separator + tableName;
  }

  private boolean serialize() {
    // T O D O
    try {
      File f = new File(getBasePersistDir());
      if (!f.exists() && !f.mkdirs()) {
        System.err.println("Failed while serializing table and dump it to disk: mkdir failed.");
        return false;
      }

      ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(getTablePersistDir()));
      for (Row row: this) { // this is iterable
        objectOutputStream.writeObject(row);
      }
      objectOutputStream.close();
      return true;
    } catch (IOException e) {
      System.err.println("Failed while serializing table: IO Exception.");
      return false;
    }
  }

  private ArrayList<Row> deserialize() {
    // T O D O
    try {
      File f = new File(getTablePersistDir());
      if (!f.exists()) { // no such file
        return new ArrayList<>();
      }

      ArrayList<Row> rows = new ArrayList<>();
      FileInputStream fileInputStream = new FileInputStream(f);
      ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
      while (fileInputStream.available() > 0) {
        rows.add((Row) objectInputStream.readObject());
      }
      objectInputStream.close();
      fileInputStream.close();
      return rows;
    } catch (IOException e) {
      System.err.println("Failed while deserializing table: IO Exception.");
    } catch (ClassNotFoundException e) {
      System.err.println("Failed while deserializing table: ClassNotFoundException.");
    }
    return new ArrayList<>();
  }

  boolean persist() {
    try {
      lock.readLock().lock();
      return serialize();
    } finally {
      lock.readLock().unlock();
    }
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
