package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Column;

import java.util.ArrayList;
import java.util.List;

class MetaInfo {

    private String tableName;
    private List<Column> columns;

    MetaInfo(String tableName, ArrayList<Column> columns) {
        this.tableName = tableName;
        this.columns = columns;
    }

    int columnFind(String name) {
        int n = columns.size();
        for (int i = 0; i < n; i++) {
            String thisColumn = columns.get(i).getName();
            if (thisColumn.equals(name))
                return i;
        }
        return -1;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }
}