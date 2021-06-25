package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.NoDatabaseSelectedException;
import cn.edu.thssdb.schema.Database;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class Session {
    private final long sessionId;
    private Database currentDatabase;
    public ArrayList<String> logList;
    private String currentDatabaseName;

    public Set<String> xTables = new HashSet<>();
    public Set<String> sTables = new HashSet<>();

    Session(long sessionId) {
        this.sessionId = sessionId;
        currentDatabase = null;
        currentDatabaseName = "";
        logList = new ArrayList<>();
    }

    public Database getCurrentDatabase() {
        if (currentDatabase == null)
            throw new NoDatabaseSelectedException();
        return currentDatabase;
    }

    public void setCurrentDatabase(Database database) {
        currentDatabase = database;
        currentDatabaseName = database.getName();
    }

    public void clearCurrentDatabase() {
        currentDatabase = null;
        currentDatabaseName = "";
    }

    public long getSessionId() {
        return sessionId;
    }

    public String getCurrentDatabaseName() {
        if (currentDatabase == null)
            throw new NoDatabaseSelectedException();
        return currentDatabaseName;
    }

    public void releaseLocks() {
        for (String tableName: xTables)
            currentDatabase.getTable(tableName).removeXLock(sessionId);
        for (String tableName: sTables)
            currentDatabase.getTable(tableName).removeSLock(sessionId);
    }

}
