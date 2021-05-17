package cn.edu.thssdb.schema;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.query.QueryTable;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Database implements Serializable {

  private String name;
  private HashMap<String, Table> tables;
  ReentrantReadWriteLock lock;

  public Database(String name) {
    this.name = name;
    this.tables = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
  }

  public boolean persist() {
    try {
      String filename = getMetaPersistFile();
      ObjectOutputStream objectOut = new ObjectOutputStream(new FileOutputStream(filename));
      for (Map.Entry<String, Table> tableEntry : tables.entrySet()) {
        objectOut.writeObject(tableEntry.getValue());
      }
      objectOut.writeObject(null);
      objectOut.close();
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean recover() {
    try {
      String filename = getMetaPersistFile();
      ObjectInputStream objectIn = new ObjectInputStream(new FileInputStream(filename));
      HashMap<String, Table> recoverTables = new HashMap<>();
      Table table = (Table) objectIn.readObject();
      while (table != null) {
        // TODO
        recoverTables.put(table.getTableName(), table);
        table = (Table) objectIn.readObject();
      }
      objectIn.close();
      tables = recoverTables;
    } catch (EOFException ignored) {
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean create(String tableName, Column[] columns) {
    if (tables.containsKey(tableName)) {
      System.err.println("Table \"" + tableName + "\" already exists");
      return false;
    }
    Table table = new Table(this.name, tableName, columns);
    tables.put(tableName, table);
    // TODO
    return persist();
//    return true;
  }

  public boolean drop(String tableName) {
    if (!tables.containsKey(tableName)) {
      System.err.println("Table \"" + tableName + "\" doesn't exist");
      return false;
    }
    tables.remove(tableName);
    // TODO
    return persist();
//    return true;
  }

  public String select(QueryTable[] queryTables) {
    // TODO
    QueryResult queryResult = new QueryResult(queryTables);
    return null;
  }

  public void quit() {
    // TODO
    for (Map.Entry<String, Table> tableEntry : tables.entrySet()) {
      tableEntry.getValue().persist();
    }
    persist();
  }

  public ArrayList<String> getTableNameList() {
    ArrayList<String> tableNames = new ArrayList<>();
    for (Map.Entry<String, Table> entry : tables.entrySet()) {
      tableNames.add(entry.getValue().getTableName());
    }
    return tableNames;
  }

  private String getMetaPersistFile() {
    File folder = new File(Global.PERSIST_PATH + File.separator + name);
    String metaFilename = Global.PERSIST_PATH + File.separator + name + File.separator + "tables" + Global.PERSIST_TABLE_META_SUFFIX;
    File persistFile = new File(metaFilename);
    if (!folder.exists() || folder.isFile()) {
      folder.mkdir();
    }
    try {
      if ((!persistFile.exists() || persistFile.isDirectory()) && !persistFile.createNewFile()) return "";
    } catch (IOException e) {
      e.printStackTrace();
    }
    return metaFilename;
  }

  public boolean hasTable(String tableName) {
    return tables.containsKey(tableName);
  }

  public String getName() {
    return name;
  }
}
