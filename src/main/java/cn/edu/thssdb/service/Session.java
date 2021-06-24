package cn.edu.thssdb.service;

import cn.edu.thssdb.exception.NoDatabaseSelectedException;
import cn.edu.thssdb.schema.Database;

import java.util.ArrayList;

public class Session {
    private long sessionId;
    private Database currentDatabase;
    public ArrayList<String> logList;
    private String currentDatabaseName;

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
        return currentDatabaseName;
    }

}
