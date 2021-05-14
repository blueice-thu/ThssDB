package cn.edu.thssdb.schema;

import cn.edu.thssdb.server.ThssDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
  private HashMap<String, Database> databases;
  private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  public static Manager getInstance() {
    return Manager.ManagerHolder.INSTANCE;
  }

  public Manager() {
    // TODO
  }

  private void createDatabaseIfNotExists() {
    // TODO
  }

  private void deleteDatabase() {
    // TODO
  }

  public void switchDatabase() {
    // TODO
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
