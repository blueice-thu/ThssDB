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
    for (Map.Entry<String, Database> databaseEntry : databases.entrySet())
      databaseEntry.getValue().recover();
  }

  public boolean createDatabaseIfNotExists(String databaseName) {
    if (!databases.containsKey(databaseName)) {
      Database database = new Database(databaseName);
      // TODO
      databases.put(databaseName, database);
      if (!database.persist()) {
        return false;
      }
      persist();
    }
    return true;
  }

  public boolean deleteDatabase(String databaseName) {
    if (!databases.containsKey(databaseName)) {
      return false;
    }
    // TODO
    File folder = new File(Global.PERSIST_PATH + File.separator + databaseName);
    if (folder.exists() && folder.isDirectory()) {
      File[] files = folder.listFiles();
      if (files != null) {
        for (File file : files) {
          file.delete();
        }
      }
      if (!folder.delete())
        return false;
    }
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

      HashMap<String, Database> recoverDatabases = new HashMap<>();
      Database database = (Database) objectIn.readObject();
      while (database != null) {
        if (!database.recover()) {
          System.err.println("Fail to load database:" + database.getName());
        }
        recoverDatabases.put(database.getName(), database);
        database = (Database) objectIn.readObject();
      }

      objectIn.close();
      databases = recoverDatabases;
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

      objectOut.writeObject(null);

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

  private String getMetaPersistFile() throws IOException {
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
