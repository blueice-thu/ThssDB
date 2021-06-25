package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.WriteFileException;
import cn.edu.thssdb.parser.statement.Statement;
import cn.edu.thssdb.utils.Global;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Manager {
    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private HashMap<String, Database> databases = new HashMap<>();
    private final HashSet<Long> transactionSessions = new HashSet<>();
    public final Logger logger = new Logger();

    public Manager() throws IOException, ClassNotFoundException {
        recover();
        for (Map.Entry<String, Database> databaseEntry : databases.entrySet())
            databaseEntry.getValue().recover();
        this.logger.redoLog(this);
    }

    public static Manager getInstance() {
        return Manager.ManagerHolder.INSTANCE;
    }

    public void createDatabaseIfNotExists(String databaseName) throws IOException {
        try {
            lock.writeLock().lock();
            if (!databases.containsKey(databaseName)) {
                Database database = new Database(databaseName);
                databases.put(databaseName, database);
                database.persist();
                persist();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void deleteDatabase(String databaseName) throws IOException {
        if (!databases.containsKey(databaseName)) {
            throw new DatabaseNotExistException(databaseName);
        }
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

        try {
            lock.writeLock().lock();
            databases.remove(databaseName);
            persist();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void recover() throws IOException, ClassNotFoundException {
        String persistFilename = getMetaPersistFile();
        try {
            ObjectInputStream objectIn = new ObjectInputStream(new FileInputStream(persistFilename));

            HashMap<String, Database> recoverDatabases = new HashMap<>();

            try {
                lock.writeLock().lock();
                Database database = (Database) objectIn.readObject();
                while (database != null) {
                    if (!database.recover()) {
                        System.err.println("Fail to load database:" + database.getName());
                    }
                    recoverDatabases.put(database.getName(), database);
                    database = (Database) objectIn.readObject();
                }
            } finally {
                lock.writeLock().unlock();
            }

            objectIn.close();
            databases = recoverDatabases;
        } catch (EOFException ignored) {
        }
    }

    public boolean persist() throws IOException {
        String persistFilename;
        persistFilename = getMetaPersistFile();
        ObjectOutputStream objectOut = new ObjectOutputStream(new FileOutputStream(persistFilename));

        try {
            lock.readLock().lock();
            for (Map.Entry<String, Database> entry : databases.entrySet()) {
                objectOut.writeObject(entry.getValue());
            }
        } finally {
            lock.readLock().unlock();
        }

        objectOut.writeObject(null);
        objectOut.close();
        return true;
    }

    public boolean hasDatabase(String databaseName) {
        try {
            lock.readLock().lock();
            return databases.containsKey(databaseName);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Database getDatabase(String databaseName) {
        try {
            lock.readLock().lock();
            return databases.getOrDefault(databaseName, null);
        } finally {
            lock.readLock().unlock();
        }
    }

    private String getMetaPersistFile() throws IOException {
        String filename = Global.PERSIST_PATH + File.separator + "databases" + Global.PERSIST_TABLE_META_SUFFIX;
        File persistFile = new File(filename);
        if ((!persistFile.exists() || persistFile.isDirectory()) && !persistFile.createNewFile()) return "";
        return filename;
    }

    public ArrayList<String> getDatabaseNameList() {
        ArrayList<String> databaseNames = new ArrayList<>();

        try {
            lock.readLock().lock();
            for (Map.Entry<String, Database> entry : databases.entrySet()) {
                databaseNames.add(entry.getValue().getName());
            }
        } finally {
            lock.readLock().unlock();
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

    public static class Logger {
        private int logCnt;
        private final ReentrantReadWriteLock lock;
        static final String DELIMITER = "|";
        static final int FLUSH_CACHE_SIZE = 100;
        static final String LOG_FILE_PATH = Global.LOG_PATH + File.separator + "log";
        public Logger() {
            lock = new ReentrantReadWriteLock();
            logCnt = 0;
        }

        public void logDatabaseStmt(ArrayList<String> logList, Statement.Type type, String dbName) {
            ArrayList<String> log = new ArrayList<>();
            assert type == Statement.Type.CREATE_DATABASE
                    || type == Statement.Type.DROP_DATABASE;
            log.add(type.toString());
            log.add(dbName);
            logList.add(String.join(DELIMITER, log));
        }

        // pass columnList only when type is CREATE_TABLE,
        //      else pass null or anything
        public void logTableStmt(
                ArrayList<String> logList,
                Statement.Type type,
                String dbName,
                String tableName,
                ArrayList<Column> columnsList) {
            ArrayList<String> log = new ArrayList<>();
            assert type == Statement.Type.CREATE_TABLE ||
                    type == Statement.Type.DROP_TABLE;
            log.add(type.toString());
            log.add(dbName);
            log.add(tableName);
            if (type == Statement.Type.CREATE_TABLE) {
                for (Column column: columnsList) {
                    log.add(column.toString());
                }
            }
            logList.add(String.join(DELIMITER, log));
        }

        public void logRowStmt(
                ArrayList<String> logList,
                Statement.Type type,
                String dbName,
                String tableName,
                ArrayList<Row> rows
                ) {
            assert rows != null;
            if (rows.size() < 1) return;
            ArrayList<String> log = new ArrayList<>();
            assert type == Statement.Type.INSERT ||
                    type == Statement.Type.DELETE ||
                    type == Statement.Type.UPDATE;
            log.add(type.toString());
            log.add(dbName);
            log.add(tableName);

            if (type == Statement.Type.INSERT) {
                // insert只有一行
                assert rows.size() == 1;
                log.add(rows.get(0).toString());
            } else if (type == Statement.Type.DELETE) {
                // delete 多行
                for (Row row: rows) {
                    log.add(row.toString());
                }
            } else {
                // update 两两配对(oRow, nRow)
                assert rows.size() % 2 == 0;
                for (Row row: rows) {
                    log.add(row.toString());
                }
            }
            logList.add(String.join(DELIMITER, log));
        }

        public void commitLog(ArrayList<String> logList, Manager manager) {
            try {
                lock.writeLock().lock();
                File logDir = new File(Global.LOG_PATH);
                if (!logDir.exists() && !logDir.mkdirs()) {
                    System.err.println("Fail to write log: mkdirs error!");
                    return;
                }
                String fName = LOG_FILE_PATH;
                FileWriter f = new FileWriter(fName, true);
                for (String s: logList) {
                    f.write(s + "\n");
                    logCnt++;
                }
                f.flush();
                f.close();
                if (logCnt >= FLUSH_CACHE_SIZE && manager.persist()) {
                    logCnt = 0;
                    File logFile = new File(fName);
                    if (logFile.exists()) {
                        logFile.delete();
                    }
                }
            } catch (Exception e) {
                System.err.println("Write Log Error");
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void redoLog(Manager manager) {
            try {
                lock.writeLock().lock();
                File f = new File(LOG_FILE_PATH);
                if (!f.exists()) {
                    return;
                }
                BufferedReader reader = new BufferedReader(new FileReader(f));
                String line;
                while ((line = reader.readLine()) != null) {
                    logCnt++;
                    String[] log = line.split("\\" + DELIMITER);
                    Statement.Type type = Statement.Type.valueOf(log[0]);
                    switch (type) {
                        case CREATE_DATABASE:
                        case DROP_DATABASE:
                            redoDatabaseStmt(manager, type, log);
                            break;
                        case CREATE_TABLE:
                        case DROP_TABLE:
                            redoTableStmt(manager, type, log);
                            break;
                        case INSERT:
                        case DELETE:
                        case UPDATE:
                            redoRowStmt(manager, type, log);
                            break;
                        default:
                            System.err.println("Error: invalid log statement type");
                    }
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            } finally {
                lock.writeLock().unlock();
            }
        }

        public void redoDatabaseStmt(Manager manager, Statement.Type type, String[] log) {
            assert type == Statement.Type.CREATE_DATABASE
                    || type == Statement.Type.DROP_DATABASE;
            try {
                if (type == Statement.Type.CREATE_DATABASE) {
                    manager.createDatabaseIfNotExists(log[1]);
                }
                else {
                    manager.deleteDatabase(log[1]);
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        public void redoTableStmt(Manager manager, Statement.Type type, String[] log) {
            assert type == Statement.Type.CREATE_TABLE ||
                    type == Statement.Type.DROP_TABLE;
            try {
                Database database = manager.getDatabase(log[1]);
                String tableName = log[2];
                if (type == Statement.Type.CREATE_TABLE) {
                    ArrayList<Column> columnsList = new ArrayList<>();
                    for (int i = 3; i < log.length; i++) {
                        columnsList.add(new Column(log[i]));
                    }
                    Column[] columns = new Column[columnsList.size()];
                    columnsList.toArray(columns);
                    database.create(tableName, columns);
                } else {
                    database.drop(tableName);
                }
            }  catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }

        public void redoRowStmt(Manager manager, Statement.Type type, String[] log) {
            assert type == Statement.Type.INSERT ||
                    type == Statement.Type.DELETE ||
                    type == Statement.Type.UPDATE;
            try {
                Database database = manager.getDatabase(log[1]);
                Table table = database.getTable(log[2]);
                if (type == Statement.Type.INSERT) {
                    table.insert(new Row(log[3], table.columns));
                } else if (type == Statement.Type.DELETE) {
                    for (int i = 3; i < log.length; i++) {
                        table.delete(new Row(log[i], table.columns));
                    }
                } else {
                    assert (log.length - 3) % 2 == 0;
                    for (int i = 3; i < log.length; i += 2) {
                        table.update(
                                new Row(log[i], table.columns),  // oRow
                                new Row(log[i+1], table.columns) // nRow
                        );
                    }
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
            }
        }
    }
}
