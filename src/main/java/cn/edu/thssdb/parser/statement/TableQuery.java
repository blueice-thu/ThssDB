package cn.edu.thssdb.parser.statement;

import java.util.ArrayList;

public class TableQuery {
    public ArrayList<Condition> conditions = null;
    public String tableNameLeft, tableNameRight = null;

    public TableQuery(String tableNameLeft) {
        this.tableNameLeft = tableNameLeft;
    }

    public TableQuery(String tableNameLeft, String tableNameRight, ArrayList<Condition> conditions) {
        this.tableNameLeft = tableNameLeft;
        this.tableNameRight = tableNameRight;
        this.conditions = conditions;
    }
}
