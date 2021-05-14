package cn.edu.thssdb.schema;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
    recover();
  }

  private void persist(Table table) {
    // TODO
    if (!table.checkMakePersistDir()) {
      System.err.println("Create folder \"" + table.getPersistDir() + "\" failed!");
    }
    try {
      FileOutputStream fileOut = new FileOutputStream(table.getMetaPersistFile());
      ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
      objectOut.writeObject(table.columns);
      objectOut.close();
      fileOut.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void persistAll() {
    for (Map.Entry<String, Table> entry : tables.entrySet()) {
      Table table = entry.getValue();
      persist(table);
    }
  }

  public void create(String name, Column[] columns) {
    // TODO
    if (tables.containsKey(name)) {
      System.out.println("Table \"" + name + "\" already exists");
      return;
    }
    Table table = new Table(this.name, name, columns);
    tables.put(name, table);
    persist(table);
  }

  public void drop() {
    // TODO
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  private void recover() {
    // TODO
  }

  public void quit() {
    // TODO
  }

  public ArrayList<String> getTableNameList() {
    ArrayList<String> tableNames = new ArrayList<>();
    for (Map.Entry<String, Table> entry : tables.entrySet()) {
      tableNames.add(entry.getValue().getTableName());
    }
    return tableNames;
  }

  public String getName() {
    return name;
  }
}
