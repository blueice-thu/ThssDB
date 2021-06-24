package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.WriteFileException;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    public HashMap<Long, List<String>> sessionSTables = new HashMap<>();
    public HashMap<Long, List<String>> sessionXTables = new HashMap<>();
    private HashMap<String, Database> databases = new HashMap<>();
    private HashSet<Long> transactionSessions = new HashSet<>();

    public Manager() throws IOException, ClassNotFoundException {
        // TODO
        recover();
        for (Map.Entry<String, Database> databaseEntry : databases.entrySet())
            databaseEntry.getValue().recover();
    }

    public static Manager getInstance() {
        return Manager.ManagerHolder.INSTANCE;
    }

    public void createDatabaseIfNotExists(String databaseName) throws IOException {
        if (!databases.containsKey(databaseName)) {
            Database database = new Database(databaseName);
            // TODO
            databases.put(databaseName, database);
            if (!database.persist()) {
                return;
            }
            persist();
        }
    }

    public void deleteDatabase(String databaseName) throws IOException {
        if (!databases.containsKey(databaseName)) {
            throw new DatabaseNotExistException(databaseName);
        }
        // TODO
        File folder = new File(Global.PERSIST_PATH + File.separator + databaseName);
        if (folder.exists() && folder.isDirectory()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (!file.delete())
                        throw new WriteFileException(file.getAbsolutePath());
                }
            }
            if (!folder.delete())
                throw new WriteFileException(folder.getAbsolutePath());
        }
        databases.remove(databaseName);
        persist();
    }

    public void recover() throws IOException, ClassNotFoundException {
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
        }
    }

    public void persist() throws IOException {
        String persistFilename = "";
        persistFilename = getMetaPersistFile();
        ObjectOutputStream objectOut = new ObjectOutputStream(new FileOutputStream(persistFilename));

        for (Map.Entry<String, Database> entry : databases.entrySet()) {
            objectOut.writeObject(entry.getValue());
        }

        objectOut.writeObject(null);
        objectOut.close();
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

    public ArrayList<String> getDatabaseNameList() {
        ArrayList<String> databaseNames = new ArrayList<>();
        for (Map.Entry<String, Database> entry : databases.entrySet()) {
            databaseNames.add(entry.getValue().getName());
        }
        return databaseNames;
    }

    // Transaction Functions
    public boolean isTransaction(Long sessionId) {
        return transactionSessions.contains(sessionId);
    }

    public void addTransaction(Long sessionId) {
        transactionSessions.add(sessionId);
    }

    public void commitTransaction(Long sessionId) {
        transactionSessions.remove(sessionId);
    }

    private static class ManagerHolder {
        private static Manager INSTANCE = null;

        static {
            try {
                INSTANCE = new Manager();
            } catch (IOException | ClassNotFoundException e) {
                e.printStackTrace();
            }
        }

        private ManagerHolder() {

        }
    }
}
