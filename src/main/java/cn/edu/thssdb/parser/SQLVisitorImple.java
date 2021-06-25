package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.service.Session;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ConstraintType;
import cn.edu.thssdb.utils.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringJoiner;

public class SQLVisitorImple extends SQLBaseVisitor {
    private Manager manager;
    private Session session;

    public SQLVisitorImple(Manager manager1, Session session1) {
        manager = manager1;
        session = session1;
    }

    @Override
    public QueryResult visitParse(SQLParser.ParseContext ctx) {
        // 助教：不考虑分号
        return visitSql_stmt(ctx.sql_stmt_list().sql_stmt().get(0));
    }

    @Override
    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        QueryResult queryResult = new QueryResult();
        String msg = "";

        try {
            if (ctx.create_table_stmt() != null) {
                SQLParser.Create_table_stmtContext ctx1 = ctx.create_table_stmt();
                msg = visitCreate_table_stmt(ctx1);
            } else if (ctx.create_db_stmt() != null) {
                msg = visitCreate_db_stmt(ctx.create_db_stmt());
            } else if (ctx.create_user_stmt() != null) {
                // TODO
                msg = "Not implemented";
            } else if (ctx.drop_db_stmt() != null) {
                msg = visitDrop_db_stmt(ctx.drop_db_stmt());
            } else if (ctx.drop_user_stmt() != null) {
                // TODO
                msg = "Not implemented";
            } else if (ctx.delete_stmt() != null) {
                msg = visitDelete_stmt(ctx.delete_stmt());
            } else if (ctx.drop_table_stmt() != null) {
                msg = visitDrop_table_stmt(ctx.drop_table_stmt());
            } else if (ctx.insert_stmt() != null) {
                msg = visitInsert_stmt(ctx.insert_stmt());
            } else if (ctx.select_stmt() != null) {
                msg = visitSelect_stmt(ctx.select_stmt());
            } else if (ctx.use_db_stmt() != null) {
                msg = visitUse_db_stmt(ctx.use_db_stmt());
            } else if (ctx.show_db_stmt() != null) {
                msg = visitShow_db_stmt(ctx.show_db_stmt());
            } else if (ctx.show_table_stmt() != null) {
                msg = visitShow_table_stmt(ctx.show_table_stmt());
            } else if (ctx.show_meta_stmt() != null) {
                msg = visitShow_meta_stmt(ctx.show_meta_stmt());
            } else if (ctx.quit_stmt() != null) {
                msg = visitQuit_stmt(ctx.quit_stmt());
            } else if (ctx.update_stmt() != null) {
                msg = visitUpdate_stmt(ctx.update_stmt());
            } else if (ctx.begin_transaction_stmt() != null) {
                msg = visitBegin_transaction_stmt(ctx.begin_transaction_stmt());
            } else if (ctx.commit_stmt() != null) {
                msg = visitCommit_stmt(ctx.commit_stmt());
            } else
                msg = "Unknown command";
        } catch (LockWaitTimeoutException e) {
            session.releaseLocks();
            if (manager.isTransaction(session.getSessionId()))
                manager.rollbackTransaction(session.getSessionId());
            msg = e.getMessage();
        } catch (Exception e) {
            msg = e.getMessage();
        }

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
                    throw new KeyNotExistException(columnName);
                }
            }
        }
        session.getCurrentDatabase().create(tableName, columns);
        manager.logger.logTableStmt(
                session.logList,
                Statement.Type.CREATE_TABLE,
                session.getCurrentDatabaseName(),
                tableName,
                new ArrayList<>(Arrays.asList(columns)));
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
        } else
            throw new UnknownTypeException(ctx);
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
        session.getCurrentDatabase().drop(tableName);
        manager.logger.logTableStmt(
                session.logList,
                Statement.Type.DROP_TABLE,
                session.getCurrentDatabaseName(),
                tableName,
                null
        );
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
        String dbName = session.getCurrentDatabaseName();
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
        try {
            currTable.getXLockWithWait(session.getSessionId());
            session.xTables.add(currTable.getTableName());

            for (int i = 0; i < numValue; i++) {
                String[] values = visitValue_entry(ctx.value_entry(i));
                ArrayList<Row> rows = new ArrayList<>();
                if (columnNames != null && columnNames.length > 0) {
                    rows.add(currTable.insert(columnNames, values));
                } else {
                    rows.add(currTable.insert(values));
                }
                manager.logger.logRowStmt(
                        session.logList,
                        Statement.Type.INSERT,
                        dbName,
                        tableName,
                        rows
                );
            }
        } catch (LockWaitTimeoutException e) {
            throw e;
        } catch (Exception e) {
            return e.getMessage();
        } finally {
            if (!manager.isTransaction(session.getSessionId()))
                currTable.removeXLock(session.getSessionId());
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

        try {
            currTable.getXLockWithWait(session.getSessionId());
            session.xTables.add(currTable.getTableName());

            ArrayList<Row> removedRows;
            String dbName = session.getCurrentDatabaseName();
            if (ctx.multiple_condition() == null) {
                removedRows = currTable.clear();
            } else {
                ArrayList<Condition> conditions = visitMultiple_condition(ctx.multiple_condition());
                QueryResult queryResult = new QueryResult(currTable);
                boolean opAnd = true;
                if (conditions.size() > 1) {
                    opAnd = ctx.multiple_condition().AND() != null;
                }
                ArrayList<Row> rowsToDelete = queryResult.getRowFromQuery(conditions, opAnd);
                if (rowsToDelete == null || rowsToDelete.size() == 0) {
                    return "No row can delete";
                }
                for (Row row : rowsToDelete) {
                    currTable.delete(row);
                }
                removedRows = rowsToDelete;
            }
            manager.logger.logRowStmt(
                    session.logList,
                    Statement.Type.DELETE,
                    dbName,
                    tableName,
                    removedRows
            );
            return "Delete succeed";
        } catch (LockWaitTimeoutException e) {
            throw e;
        } catch (Exception e) {
            return e.getMessage();
        } finally {
            if (!manager.isTransaction(session.getSessionId()))
                currTable.removeXLock(session.getSessionId());
        }
    }

    @Override
    public ArrayList<Condition> visitMultiple_condition(SQLParser.Multiple_conditionContext ctx) {
        if (ctx == null) return null;
        ArrayList<Condition> ret = new ArrayList<>();
        if (ctx.condition() != null) {
            ret.add(visitCondition(ctx.condition()));
        } else {
            ret.addAll(visitMultiple_condition(ctx.multiple_condition(0)));
            ret.addAll(visitMultiple_condition(ctx.multiple_condition(1)));
        }
        return ret;

    }

    @Override
    public String visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        String databaseName = ctx.database_name().getText();
        if (databaseName.equals("<missing IDENTIFIER>")) {
            return new EmptyValueException("database_name").getMessage();
        }
        String dbName = ctx.database_name().getText();
        try {
            if (manager.hasDatabase(dbName))
                throw new DatabaseAlreadyExistException(dbName);
            manager.createDatabaseIfNotExists(dbName);
            manager.logger.logDatabaseStmt(session.logList, Statement.Type.CREATE_DATABASE, dbName);
            return "Create database succeed";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    @Override
    public String visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        try {
            if (ctx.database_name() == null || ctx.database_name().getText().equals(""))
                throw new EmptyValueException("database_name");
            String dbName = ctx.database_name().getText();
            manager.deleteDatabase(dbName);
            manager.logger.logDatabaseStmt(session.logList, Statement.Type.DROP_DATABASE, dbName);
            return "Drop database succeed";
        } catch (DatabaseNotExistException e) {
            if (ctx.K_EXISTS() == null) {
                return "Database doesn't exist";
            }
            return e.getMessage();
        } catch (Exception e) {
            return e.getMessage();
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

//        Long sessionId = session.getSessionId();
//        if (manager.isTransaction(sessionId)) {
//            for (String tableName : manager.sessionXTables.get(sessionId)) {
//                session.getCurrentDatabase().getTable(tableName).removeXLock(sessionId);
//            }
//            for (String tableName : manager.sessionSTables.get(sessionId)) {
//                session.getCurrentDatabase().getTable(tableName).removeSLock(sessionId);
//            }
//            manager.sessionSTables.remove(sessionId);
//            manager.sessionXTables.remove(sessionId);
//        }

        session.releaseLocks();

        for (String db : dbList) {
            manager.getDatabase(db).quit();
        }
        return "Quit";
    }

    @Override
    public String visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        String tableName = ctx.table_name().getText().toLowerCase();
        Table currTable = session.getCurrentDatabase().getTable(tableName);

        try {
            currTable.getXLockWithWait(session.getSessionId());
            session.xTables.add(currTable.getTableName());

            ArrayList<Condition> conditions = visitMultiple_condition(ctx.multiple_condition());
            QueryResult queryResult = new QueryResult(currTable);
            boolean opAnd = true;
            if (conditions.size() > 1) {
                opAnd = ctx.multiple_condition().AND() != null;
            }
            ArrayList<Row> rowsToUpdate = queryResult.getRowFromQuery(conditions, opAnd);
            if (rowsToUpdate == null || rowsToUpdate.size() == 0) {
                return "No row can update";
            }
            String dbName = session.getCurrentDatabaseName();
            ArrayList<Row> rows = new ArrayList<>();
            for (Row row : rowsToUpdate) {
                String columnName = ctx.column_name().getText().toLowerCase();
                Pair<Row, Row> rowRowPair = currTable.update(row, columnName, ctx.expression().getText());
                rows.add(rowRowPair.getLeft());
                rows.add(rowRowPair.getRight());
            }
            manager.logger.logRowStmt(
                    session.logList,
                    Statement.Type.UPDATE,
                    dbName,
                    tableName,
                    rows
            );
        } catch (Exception e) {
            return e.getMessage();
        } finally {
            if (!manager.isTransaction(session.getSessionId()))
                currTable.removeXLock(session.getSessionId());
        }
        return "Update succeed";
    }


    /*
        table_query:
            table_name
            | table_name ( K_JOIN table_name )+ K_ON multiple_condition ;
     */
    @Override
    public TableQuery visitTable_query(SQLParser.Table_queryContext ctx) {
        if (ctx.table_name(1) == null) {
            return new TableQuery(ctx.table_name(0).getText().toLowerCase());
        } else {
            return new TableQuery(
                    ctx.table_name(0).getText().toLowerCase(),
                    ctx.table_name(1).getText().toLowerCase(),
                    visitMultiple_condition(ctx.multiple_condition()));
        }
    }

    /*
        select_stmt :
            K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
                K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;
     */
    @Override
    public String visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        // column names
        ArrayList<ColumnFullName> resultColumnNameList = new ArrayList<>();
        List<SQLParser.Result_columnContext> columnContextList = ctx.result_column();
        for (SQLParser.Result_columnContext columnContext : columnContextList) {
            resultColumnNameList.add(visitResult_column(columnContext));
        }

        TableQuery tableQuery = visitTable_query(ctx.table_query(0));

        // condition
        ArrayList<Condition> conditions = null;
        if (ctx.multiple_condition() != null) {
            conditions = visitMultiple_condition(ctx.multiple_condition());
        }

        Database database = session.getCurrentDatabase();
        Table currTable = database.getTable(tableQuery.tableNameLeft);

        try {
            currTable.getSLockWithWait(session.getSessionId());
            session.sTables.add(currTable.getTableName());

            ArrayList<Table> tables2Query = new ArrayList<>();
            tables2Query.add(currTable);
            boolean opAndOn = true;
            if (tableQuery.conditions != null && tableQuery.conditions.size() > 1) {
                opAndOn = ctx.table_query(0).multiple_condition().AND() != null;
            }
            boolean opAnd = true;
            if (conditions != null && conditions.size() > 1) {
                opAnd = ctx.multiple_condition().AND() != null;
            }
            if (tableQuery.tableNameRight != null) {
                tables2Query.add(database.getTable(tableQuery.tableNameRight));
                QueryResult queryResult = new QueryResult(tables2Query, tableQuery.conditions, opAndOn);
                return queryResult.selectQuery(resultColumnNameList, conditions, opAnd);
            } else {
                QueryResult queryResult = new QueryResult(tables2Query);
                return queryResult.selectQuery(resultColumnNameList, conditions, opAnd);
            }
        } catch (LockWaitTimeoutException e) {
            throw e;
        } catch (Exception e) {
            return e.getMessage();
        } finally {
            if (!manager.isTransaction(session.getSessionId()))
                currTable.removeSLock(session.getSessionId());
        }
    }

    @Override
    public ColumnFullName visitResult_column(SQLParser.Result_columnContext ctx) {
        // null="*"
        ColumnFullName columnFullName;
        if (ctx.getText().equals("*")) {
            columnFullName = new ColumnFullName(null, null);
        } else if (ctx.table_name() != null) {
            String tableName = ctx.table_name().getText().toLowerCase();
            columnFullName = new ColumnFullName(tableName, null);
        } else {
            columnFullName = visitColumn_full_name(ctx.column_full_name());
        }
        return columnFullName;
    }

    @Override
    public Condition visitCondition(SQLParser.ConditionContext ctx) {
        return new Condition(
                visitExpression(ctx.expression(0)),
                ctx.comparator().getText(),
                visitExpression(ctx.expression(1)));
    }

    @Override
    public Expression visitExpression(SQLParser.ExpressionContext ctx) {
        Expression expression;
        if (ctx.comparer() != null) {
            expression = new Expression(visitComparer(ctx.comparer()));
        } else if (ctx.expression(1) != null) {
            Comparer comparerLeft = (visitExpression(ctx.expression(0))).comparerLeft;
            Comparer comparerRight = (visitExpression(ctx.expression(1))).comparerLeft;
            if (ctx.MUL() != null) {
                expression = new Expression(comparerLeft, Expression.OP.MUL, comparerRight);
            } else if (ctx.DIV() != null) {
                expression = new Expression(comparerLeft, Expression.OP.DIV, comparerRight);
            } else if (ctx.ADD() != null) {
                expression = new Expression(comparerLeft, Expression.OP.ADD, comparerRight);
            } else {
                expression = new Expression(comparerLeft, Expression.OP.SUB, comparerRight);
            }
        } else {
            expression = visitExpression(ctx.expression(0));
        }
        return expression;
    }

    @Override
    public Comparer visitComparer(SQLParser.ComparerContext ctx) {
        Comparer comparer;
        if (ctx.column_full_name() != null) {
            comparer = visitColumn_full_name(ctx.column_full_name());
        } else {
            comparer = visitLiteral_value(ctx.literal_value());
        }
        return comparer;
    }

    @Override
    public ColumnFullName visitColumn_full_name(SQLParser.Column_full_nameContext ctx) {
        String tableName = null;
        if (ctx.table_name() != null) {
            tableName = ctx.table_name().getText().toLowerCase();
        }
        String columnName = ctx.column_name().getText().toLowerCase();
        return new ColumnFullName(tableName, columnName);
    }

    @Override
    public LiteralValue visitLiteral_value(SQLParser.Literal_valueContext ctx) {
        Comparable value = null;
        if (ctx.NUMERIC_LITERAL() != null) {
            String string = ctx.getText();
            if (string.contains(".") || string.contains("e")) {
                value = Double.valueOf(string);
            } else {
                value = Long.valueOf(string);
            }
        } else if (ctx.STRING_LITERAL() != null) {
            value = ctx.getText();
        }
        return new LiteralValue(value);
    }

    @Override
    public String visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        if (ctx.database_name() == null || ctx.database_name().getText().length() == 0) {
            return "Empty database name";
        }
        String dbName = ctx.database_name().getText();
        if (manager.hasDatabase(dbName)) {
            session.setCurrentDatabase(manager.getDatabase(dbName));
        } else {
            return "No such database: " + dbName;
        }
        return "Switch to " + dbName;

    }

    @Override
    public String visitBegin_transaction_stmt(SQLParser.Begin_transaction_stmtContext ctx) {
        if (manager.isTransaction(session.getSessionId())) {
            return "Already in transaction";
        }
        manager.addTransaction(session.getSessionId());
        return "Start transaction";
    }

    @Override
    public String visitCommit_stmt(SQLParser.Commit_stmtContext ctx) {
        if (!manager.isTransaction(session.getSessionId())) {
            return "Not in transaction";
        }
        session.releaseLocks();
        manager.logger.commitLog(session.logList, manager);
        manager.commitTransaction(session.getSessionId());
        return "Commit successfully";
    }

    @Override
    public String visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        if (ctx.table_name() == null || ctx.table_name().getText().isEmpty())
            throw new EmptyValueException("table_name");
        String tableName = ctx.table_name().getText();
        Table currTable = session.getCurrentDatabase().getTable(tableName);
        StringJoiner joiner = new StringJoiner("\n");
        joiner.add(String.format("%-8s | %-8s | %-8s | %-8s | %-8s", "name", "type", "primary", "notNull", "maxLength"));
        for (Column column : currTable.columns) {
            joiner.add(column.toFormatString());
        }
        return joiner.toString();
    }
}
