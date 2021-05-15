package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases = new HashMap<>();
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
    recover();
  }

  public boolean createDatabaseIfNotExists(String databaseName) {
    if (!databases.containsKey(databaseName)) {
      Database database = new Database(databaseName);
      // TODO
      databases.put(databaseName, database);
      persist();
    }
    return true;
  }

  public boolean deleteDatabase(String databaseName) {
    if (!databases.containsKey(databaseName)) {
      return false;
    }
    Database database = databases.get(databaseName);
    // TODO
    databases.remove(databaseName);
    persist();
    return true;
  }

  public void switchDatabase() {
    // TODO
  }

  public boolean recover() {
    String persistFilename = "";
    try {
      persistFilename = getMetaPersistFile();
      ObjectInputStream objectIn = new ObjectInputStream(new FileInputStream(persistFilename));

      HashMap<String, Database> recoverDatabase = new HashMap<>();
      while (objectIn.available() > 0) {
        Database database = (Database) objectIn.readObject();
        recoverDatabase.put(database.getName(), database);
      }

      objectIn.close();
      databases = recoverDatabase;
    } catch (EOFException ignored) {
    } catch (IOException | ClassNotFoundException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean persist() {
    String persistFilename = "";
    try {
      persistFilename = getMetaPersistFile();
      ObjectOutputStream objectOut = new ObjectOutputStream(new FileOutputStream(persistFilename));

      for (Map.Entry<String, Database> entry : databases.entrySet()) {
        objectOut.writeObject(entry.getValue());
      }

      objectOut.close();
    } catch (IOException e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  public boolean hasDatabase(String databaseName) {
    return databases.containsKey(databaseName);
  }

  public Database getDatabase(String databaseName) {
    return databases.getOrDefault(databaseName, null);
  }

  public String getMetaPersistFile() throws IOException {
    String filename = Global.PERSIST_PATH + File.separator + "databases" + Global.PERSIST_TABLE_META_SUFFIX;
    File persistFile = new File(filename);
    if ((!persistFile.exists() || persistFile.isDirectory()) && !persistFile.createNewFile()) return "";
    return filename;
  }

  private static class ManagerHolder {
    private static final Manager INSTANCE = new Manager();
    private ManagerHolder() {

    }
  }

  public ArrayList<String> getDatabaseNameList() {
    ArrayList<String> databaseNames = new ArrayList<>();
    for (Map.Entry<String, Database> entry : databases.entrySet()) {
      databaseNames.add(entry.getValue().getName());
    }
    return databaseNames;
  }
}
