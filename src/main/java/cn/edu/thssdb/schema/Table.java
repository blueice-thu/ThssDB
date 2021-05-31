package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateKeyException;
import cn.edu.thssdb.exception.KeyNotExistException;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.index.BPlusTreeIterator;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.utils.Global;
import cn.edu.thssdb.utils.Pair;
import cn.edu.thssdb.utils.StringHelper;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Table implements Iterable<Row>, Serializable {
    ReentrantReadWriteLock lock;
    private String databaseName;

    private String tableName;
    public ArrayList<Column> columns;
    public BPlusTree<Entry, Row> index;
    private int primaryIndex;

    public Table(String databaseName, String tableName, Column[] columns) {
        // T O D O
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<>();
        if (columns != null)
            this.columns.addAll(Arrays.asList(columns));
        this.index = new BPlusTree<>();
        this.lock = new ReentrantReadWriteLock();

        // init primary index
        this.primaryIndex = -1;
        int len = this.columns.size();
        for (int i = 0; i < len; i++) {
            if (this.columns.get(i).isPrimary()) {
                this.primaryIndex = i;
                break;
            }
        }
        if (this.primaryIndex == -1 && len > 0) {
            // no primary, set one
            this.columns.get(0).setPrimary();
            this.primaryIndex = 0;
        }
    }




    public boolean persist() {
        try {
            lock.readLock().lock();
            return serialize();
        } finally {
            lock.readLock().unlock();
        }
    }

    public void recover() {
        try {
            lock.writeLock().lock();
            ArrayList<Row> rows = deserialize();
            if (!rows.isEmpty()) {
                for (Row row : rows) {
                    index.put(row.getEntries().get(primaryIndex), row);
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void insert(Row row) throws DuplicateKeyException {
        Entry primary_key = row.getEntries().get(primaryIndex);
        boolean has_exist;
        try {
            lock.readLock().lock();
            has_exist = index.contains(primary_key);
        } finally {
            lock.readLock().unlock();
        }
        if (!has_exist) {
            try {
                lock.writeLock().lock();
                index.put(primary_key, row);
            } finally {
                lock.writeLock().unlock();
            }
        } else {
            throw new DuplicateKeyException();
        }
    }

    public void insert(String[] columnNames, String[] values) throws Exception {
        if (columnNames == null || values == null) {
            throw new Exception("TODO");
        }
        if (columnNames.length != values.length) {
            throw new Exception("TODO");
        }
        ArrayList<Entry> entries = new ArrayList<>();
        for (Column column : columns) {
            int index = StringHelper.indexOf(columnNames, column.getName());
            Comparable realValue = null;
            if (index == -1) {
                if (column.isNotNull()) {
                    throw new Exception("TODO");
                }
            }
            else if (values[index].equals("null")) {
                if (column.isNotNull()) {
                    throw new Exception("TODO");
                }
            }
            else {
                switch (column.getType()) {
                    case INT:
                        realValue = Integer.parseInt(values[index]);
                        break;
                    case LONG:
                        realValue = Long.parseLong(values[index]);
                        break;
                    case FLOAT:
                        realValue = Float.parseFloat(values[index]);
                        break;
                    case DOUBLE:
                        realValue = Double.parseDouble(values[index]);
                        break;
                    case STRING:
                        int n = values[index].length();
                        if (values[index].charAt(0) != '\'' || values[index].charAt(n - 1) != '\'') {
                            throw new Exception("TODO");
                        }
                        realValue = values[index].substring(1, n - 1);
                        break;
                    default:
                        throw new Exception("TODO");
                }
            }
            Entry entry = new Entry(realValue);
            entries.add(entry);
        }
        insert(new Row(entries));
    }

    public void insert(String[] values) throws Exception {
        if (values == null || values.length == 0) {
            throw new Exception("TODO");
        }
        int nValues = values.length;
        if (nValues != columns.size()) {
            throw new Exception("TODO");
        }
        Entry[] entries = new Entry[nValues];
        for (int i = 0; i < nValues; i++) {
            Column column = columns.get(i);
            Comparable realValue = null;
            if (values[i].equals("null")) {
                if (column.isNotNull()) {
                    throw new Exception("TODO");
                }
            }
            else {
                switch (column.getType()) {
                    case INT:
                        realValue = Integer.parseInt(values[i]);
                        break;
                    case LONG:
                        realValue = Long.parseLong(values[i]);
                        break;
                    case FLOAT:
                        realValue = Float.parseFloat(values[i]);
                        break;
                    case DOUBLE:
                        realValue = Double.parseDouble(values[i]);
                        break;
                    case STRING:
                        int n = values[i].length();
                        if (values[i].charAt(0) != '\'' || values[i].charAt(n - 1) != '\'') {
                            throw new Exception("TODO");
                        }
                        realValue = values[i].substring(1, n - 1);
                        break;
                    default:
                        throw new Exception("TODO");
                }
            }
            entries[i] = new Entry(realValue);
        }
        insert(new Row(entries));
    }

    public void delete(Row row) throws KeyNotExistException {
        Entry primary_key = row.getEntries().get(primaryIndex);
        boolean has_exist;
        try {
            lock.readLock().lock();
            has_exist = index.contains(primary_key);
        } finally {
            lock.readLock().unlock();
        }
        if (has_exist) {
            try {
                lock.writeLock().lock();
                Entry entry = row.getEntries().get(primaryIndex);
                index.remove(entry);
            } finally {
                lock.writeLock().unlock();
            }
        } else {
            throw new KeyNotExistException();
        }
    }

    public void clear() {
        for (Pair<Entry, Row> entryRowPair : index) {
            index.remove(entryRowPair.getLeft());
        }
    }

    public void update(Row row) throws Exception {
        Entry primary_key = row.getEntries().get(primaryIndex);
        boolean has_exist;
        try {
            lock.readLock().lock();
            has_exist = index.contains(primary_key);
        } finally {
            lock.readLock().unlock();
        }
        if (has_exist) {
            try {
                lock.writeLock().lock();
                Entry entry = row.getEntries().get(primaryIndex);
                index.update(entry, row);
            } finally {
                lock.writeLock().unlock();
            }
        } else {
            throw new Exception("Row has not existed!");
        }
    }

    public void addColumn(String name, String type, int maxLen) throws Exception {
        // 检查是否存在
        for (Column c : columns) {
            if (c.getName().equals(name)) {
                throw new Exception("column has existed.");
            }
        }
        // 增加属性
        ColumnType columnType = null;
        try {
            columnType = ColumnType.valueOf(type);
        } catch (Exception e) {
            throw new Exception("column type is invalid.");
        }
        columns.add(new Column(name, columnType, 0, false, maxLen));
        if (columns.size() == 1) {
            this.columns.get(0).setPrimary();
            this.primaryIndex = 0;
        }
        // 更新数据
        for (Row row : this) {
            row.getEntries().add(new Entry(null));
        }
    }

    public void dropColumn(String name) throws Exception {
        for (int i = 0; i < columns.size(); i++) {
            Column c = columns.get(i);
            if (c.getName().equals(name)) {
                // 主键无法删除
                if (c.isPrimary()) {
                    throw new Exception("Primary key can not be delete.");
                }
                columns.remove(i);
                // 更新数据
                for (Row row : this) {
                    row.getEntries().remove(i);
                }
                return;
            }
        }
        throw new Exception("column does not exist.");
    }

    public void alterColumn(String name, String type, int maxLen) throws Exception {
        ColumnType columnType = null;
        try {
            columnType = ColumnType.valueOf(type);
        } catch (Exception e) {
            throw new Exception("column type is invalid.");
        }
        for (Column c : columns) {
            if (c.getName().equals(name)) {
                c.setType(columnType);
                return;
            }
        }
        throw new Exception("column does not exist.");
    }

    public String getPersistDir() {
        return Global.PERSIST_PATH + File.separator + databaseName + File.separator + tableName;
    }

    public String getRowsPersistFile() {
        return getPersistDir() + File.separator + tableName + Global.PERSIST_TABLE_ROWS_SUFFIX;
    }

    public boolean checkMakePersistDir() {
        File tableFolder = new File(getPersistDir());
        return (tableFolder.exists() && !tableFolder.isFile()) || tableFolder.mkdirs();
    }

    private boolean serialize() {
        // T O D O
        try {
            if (checkMakePersistDir()) {
                System.err.println("Failed while serializing table and dump it to disk: mkdir failed.");
                return false;
            }

            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(getRowsPersistFile()));
            for (Row row : this) { // this is iterable
                objectOutputStream.writeObject(row);
            }
            objectOutputStream.close();
            return true;
        } catch (IOException e) {
            System.err.println("Failed while serializing table: IO Exception.");
            return false;
        }
    }

    private ArrayList<Row> deserialize() {
        // T O D O
        try {
            // Judge whether file "data/{databaseName}/tables/{tableName}.data" exists
            File tableFile = new File(getRowsPersistFile());
            if (!tableFile.exists() || tableFile.isDirectory()) {
                return new ArrayList<>();
            }

            ArrayList<Row> rows = new ArrayList<>();
            FileInputStream fileInputStream = new FileInputStream(tableFile);
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            while (fileInputStream.available() > 0) {
                rows.add((Row) objectInputStream.readObject());
            }
            objectInputStream.close();
            fileInputStream.close();
            return rows;
        } catch (IOException e) {
            System.err.println("Failed while deserializing table: IO Exception.");
        } catch (ClassNotFoundException e) {
            System.err.println("Failed while deserializing table: ClassNotFoundException.");
        }
        return new ArrayList<>();
    }


    private class TableIterator implements Iterator<Row> {
        private Iterator<Pair<Entry, Row>> iterator;

        TableIterator(Table table) {
            this.iterator = table.index.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Row next() {
            return iterator.next().right;
        }
    }

    @Override
    public Iterator<Row> iterator() {
        return new TableIterator(this);
    }

    // Getter and Setter
    public String getTableName() {
        return tableName;
    }

    public void readLock(){
        lock.readLock().lock();
    }

    public void readUnlock(){
        lock.readLock().unlock();
    }

    public void writeLock(){
        lock.writeLock().lock();
    }

    public void writeUnlock(){
        lock.writeLock().unlock();
    }
}
