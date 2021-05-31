package cn.edu.thssdb.parser;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.service.Session;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ConstraintType;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.StringJoiner;

public class SQLVisitorImple extends SQLBaseVisitor {
    private Manager manager;
    private Session session;

    SQLVisitorImple(Manager manager1, Session session1) {
        manager = manager1;
        session = session1;
    }

    @Override
    public Object visitParse(SQLParser.ParseContext ctx) {
        // 助教：不考虑分号
        return visitSql_stmt(ctx.sql_stmt_list().sql_stmt().get(0));
    }

    @Override
    public Object visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        QueryResult queryResult = new QueryResult();
        String msg = "";

        if (ctx.create_table_stmt() != null) {
            SQLParser.Create_table_stmtContext ctx1 = ctx.create_table_stmt();
            msg = visitCreate_table_stmt(ctx1);
        } else if (ctx.create_db_stmt() != null) {
            msg = visitCreate_db_stmt(ctx.create_db_stmt());
        } else if (ctx.create_user_stmt() != null) {
            // TODO
        } else if (ctx.drop_db_stmt() != null) {
            msg = visitDrop_db_stmt(ctx.drop_db_stmt());
        } else if (ctx.drop_user_stmt() != null) {
            // TODO
        } else if (ctx.delete_stmt() != null) {
            msg = visitDelete_stmt(ctx.delete_stmt());
        } else if (ctx.drop_table_stmt() != null) {
            msg = visitDrop_table_stmt(ctx.drop_table_stmt());
        } else if (ctx.insert_stmt() != null) {
            msg = visitInsert_stmt(ctx.insert_stmt());
        } else if (ctx.select_stmt() != null) {
            // TODO
        } else if (ctx.create_view_stmt() != null) {
            // TODO
        } else if (ctx.drop_view_stmt() != null) {
            // TODO
        } else if (ctx.grant_stmt() != null) {
            // TODO
        } else if (ctx.revoke_stmt() != null) {
            // TODO
        } else if (ctx.use_db_stmt() != null) {
            // TODO
        } else if (ctx.show_db_stmt() != null) {
            msg = visitShow_db_stmt(ctx.show_db_stmt());
        } else if (ctx.show_table_stmt() != null) {
            msg = visitShow_table_stmt(ctx.show_table_stmt());
        } else if (ctx.show_meta_stmt() != null) {
            // TODO
        } else if (ctx.quit_stmt() != null) {
            visitQuit_stmt(ctx.quit_stmt());
        } else if (ctx.update_stmt() != null) {
            // TODO
        } else
            return null;
        queryResult.setMsg(msg);
        return queryResult;
    }

    @Override
    public String visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();

        // Process column defines
        int numColumns = ctx.column_def().size();
        Column[] columns = new Column[numColumns];
        for (int i = 0; i < numColumns; i++) {
            columns[i] = visitColumn_def(ctx.column_def(i));
        }

        // Process table constraint: primary
        if (ctx.table_constraint() != null) {
            String[] columnNames = visitTable_constraint(ctx.table_constraint());
            for (String columnName : columnNames) {
                String targetColumn = columnName.toLowerCase();
                boolean flag = false;
                for (int i = 0; i < numColumns; i++) {
                    if (columns[i].getName().toLowerCase().equals(targetColumn)) {
                        flag = true;
                        columns[i].setPrimary();
                        break;
                    }
                }
                if (!flag) {
                    // TODO: Error
                    return null;
                }
            }
        }

        try {
            session.getCurrentDatabase().create(tableName, columns);
        } catch (Exception e) {
            return e.toString();
        }

        return "Create table " + tableName + " successfully.";
    }

    @Override
    public Column visitColumn_def(SQLParser.Column_defContext ctx) {
        String name = ctx.column_name().getText().toLowerCase();
        Pair<ColumnType, Integer> columnType = visitType_name(ctx.type_name());
        int primary = 0;
        boolean notNull = false;

        int numConstraint = ctx.column_constraint().size();
        for (int i = 0; i < numConstraint; i++) {
            ConstraintType constraintType = visitColumn_constraint(ctx.column_constraint(i));
            if (constraintType.equals(ConstraintType.PRIMARY)) {
                primary = 1;
            } else if (constraintType.equals(ConstraintType.NOTNULL)) {
                notNull = true;
            }
            if (primary == 1) {
                notNull = true;
            }
        }

        return new Column(name, columnType.getLeft(), primary, notNull, columnType.getRight());
    }

    @Override
    public Pair<ColumnType, Integer> visitType_name(SQLParser.Type_nameContext ctx) {
        if (ctx.T_INT() != null) {
            return new Pair<>(ColumnType.INT, 0);
        } else if (ctx.T_LONG() != null) {
            return new Pair<>(ColumnType.LONG, 0);
        } else if (ctx.T_FLOAT() != null) {
            return new Pair<>(ColumnType.FLOAT, 0);
        } else if (ctx.T_DOUBLE() != null) {
            return new Pair<>(ColumnType.DOUBLE, 0);
        } else if (ctx.T_STRING() != null) {
            return new Pair<>(ColumnType.STRING, Integer.parseInt(ctx.NUMERIC_LITERAL().getText()));
            // TODO: Error
        } else
            return null;
    }

    @Override
    public ConstraintType visitColumn_constraint(SQLParser.Column_constraintContext ctx) {
        if (ctx.K_PRIMARY() != null) {
            return ConstraintType.PRIMARY;
        } else if (ctx.K_NULL() != null) {
            return ConstraintType.NOTNULL;
        } else
            return null;
    }

    @Override
    public String[] visitTable_constraint(SQLParser.Table_constraintContext ctx) {
        int numColumns = ctx.column_name().size();
        String[] columnNames = new String[numColumns];
        for (int i = 0; i < numColumns; i++) {
            columnNames[i] = ctx.column_name(i).getText().toLowerCase();
        }
        return columnNames;
    }

    @Override
    public String visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        try {
            session.getCurrentDatabase().drop(tableName);
        } catch (Exception e) {
            return e.toString();
        }
        return "Drop table " + tableName + " successfully.";
    }

    @Override
    public String visitShow_table_stmt(SQLParser.Show_table_stmtContext ctx) {
        StringJoiner joiner = new StringJoiner("\n");
        ArrayList<String> tableNames = session.getCurrentDatabase().getTableNameList();
        for (String tableName : tableNames)
            joiner.add(tableName);
        return joiner.toString();
    }

    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        Table currTable = session.getCurrentDatabase().getTable(tableName);

        String[] columnNames = null;
        if (ctx.column_name() != null) {
            int numColumnNames = ctx.column_name().size();
            columnNames = new String[numColumnNames];
            for (int i = 0; i < numColumnNames; i++) {
                columnNames[i] = ctx.column_name(i).getText().toLowerCase();
            }
        }

        int numValue = ctx.value_entry().size();
        for (int i = 0; i < numValue; i++) {
            String[] values = visitValue_entry(ctx.value_entry(i));
            try {
                if (columnNames != null) {
                    currTable.insert(columnNames, values);
                }
                else {
                    currTable.insert(values);
                }
            } catch (Exception e) {
                System.err.println(e);
            }

        }
        return "Insert succeed";
    }

    @Override
    public String[] visitValue_entry(SQLParser.Value_entryContext ctx) {
        int numLiteral = ctx.literal_value().size();
        String[] value = new String[numLiteral];
        for (int i = 0; i < numLiteral; i++) {
            value[i] = ctx.literal_value(i).getText();
        }
        return value;
    }

    @Override
    public String visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        Table currTable = session.getCurrentDatabase().getTable(tableName);
        if (ctx.multiple_condition() == null) {
            currTable.clear();
        }
        else {
            QueryResult queryResult = new QueryResult(currTable);
        }
        return "Delete succeed";
    }

    @Override
    public Object visitMultiple_condition(SQLParser.Multiple_conditionContext ctx) {

        // TODO
        return super.visitMultiple_condition(ctx);
    }

    @Override
    public String visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        if (ctx.database_name() != null || ctx.database_name().getText().equals("")) {
            return "Empty database name";
        }
        String dbName = ctx.database_name().getText();
        if (manager.hasDatabase(dbName)) {
            return "Database exists";
        }
        if (manager.createDatabaseIfNotExists(dbName)) {
            return "Create database succeed";
        }
        return "Create database failed";
    }

    @Override
    public String visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        if (ctx.database_name() != null || ctx.database_name().getText().equals("")) {
            return "Empty database name";
        }
        String dbName = ctx.database_name().getText();
        if (!manager.hasDatabase(dbName)) {
            if (ctx.K_EXISTS() == null) {
                return "Database doesn't exist";
            }
            return "";
        }
        else {
            if (manager.deleteDatabase(dbName)) {
                return "Drop database succeed";
            }
            return "Drop database failed";
        }
    }

    @Override
    public String visitShow_db_stmt(SQLParser.Show_db_stmtContext ctx) {
        StringJoiner joiner = new StringJoiner("\n");
        ArrayList<String> databaseNames = manager.getDatabaseNameList();
        for (String databaseName : databaseNames)
            joiner.add(databaseName);
        return joiner.toString();
    }

    @Override
    public String visitQuit_stmt(SQLParser.Quit_stmtContext ctx) {
        ArrayList<String> dbList = manager.getDatabaseNameList();
        for (String db : dbList) {
            manager.getDatabase(db).quit();
        }
        return "Quit";
    }
}
