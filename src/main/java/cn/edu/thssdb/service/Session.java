package cn.edu.thssdb.service;

import cn.edu.thssdb.schema.Database;

public class Session {
    private long sessionId;
    private Database currentDatabase;

    private String currentDatabaseName;

    Session(long sessionId) {
        this.sessionId = sessionId;
        currentDatabase = null;
        currentDatabaseName = "";
    }

    public Database getCurrentDatabase() {
        return currentDatabase;
    }

    public void setCurrentDatabase(Database database) {
        currentDatabase = database;
        currentDatabaseName = database.getName();
    }

    public long getSessionId() {
        return sessionId;
    }

    public String getCurrentDatabaseName() {
        return currentDatabaseName;
    }

}
