package cn.edu.thssdb.parser;

import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.service.Session;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ConstraintType;
import cn.edu.thssdb.utils.Pair;

import java.util.Locale;

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
        if (ctx.create_table_stmt() != null) {
            // TODO
            SQLParser.Create_table_stmtContext ctx1 = ctx.create_table_stmt();
            String msg = visitCreate_table_stmt(ctx1);
            QueryResult queryResult = new QueryResult(null);
            queryResult.setMsg(msg);
            return queryResult;
        }
        else if (ctx.create_db_stmt() != null) {
            // TODO
        }
        else if (ctx.create_user_stmt() != null) {
            // TODO
        }
        else if (ctx.drop_db_stmt() != null) {
            // TODO
        }
        else if (ctx.drop_user_stmt() != null) {
            // TODO
        }
        else if (ctx.delete_stmt() != null) {
            // TODO
        }
        else if (ctx.drop_table_stmt() != null) {
            // TODO
        }
        else if (ctx.insert_stmt() != null) {
            // TODO
        }
        else if (ctx.select_stmt() != null) {
            // TODO
        }
        else if (ctx.create_view_stmt() != null) {
            // TODO
        }
        else if (ctx.drop_view_stmt() != null) {
            // TODO
        }
        else if (ctx.grant_stmt() != null) {
            // TODO
        }
        else if (ctx.revoke_stmt() != null) {
            // TODO
        }
        else if (ctx.use_db_stmt() != null) {
            // TODO
        }
        else if (ctx.show_db_stmt() != null) {
            // TODO
        }
        else if (ctx.show_table_stmt() != null) {
            // TODO
        }
        else if (ctx.show_meta_stmt() != null) {
            // TODO
        }
        else if (ctx.quit_stmt() != null) {
            // TODO
        }
        else if (ctx.update_stmt() != null) {
            // TODO
        }
        else
            return null;
        return super.visitSql_stmt(ctx);
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
            }
            else if (constraintType.equals(ConstraintType.NOTNULL)) {
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
        }
        else if (ctx.T_LONG() != null) {
            return new Pair<>(ColumnType.LONG, 0);
        }
        else if (ctx.T_FLOAT() != null) {
            return new Pair<>(ColumnType.FLOAT, 0);
        }
        else if (ctx.T_DOUBLE() != null) {
            return new Pair<>(ColumnType.DOUBLE, 0);
        }
        else if (ctx.T_STRING() != null) {
            return new Pair<>(ColumnType.STRING, Integer.parseInt(ctx.NUMERIC_LITERAL().getText()));
            // TODO: Error
        }
        else
            return null;
    }

    @Override
    public ConstraintType visitColumn_constraint(SQLParser.Column_constraintContext ctx) {
        if (ctx.K_PRIMARY() != null) {
            return ConstraintType.PRIMARY;
        }
        else if (ctx.K_NULL() != null) {
            return ConstraintType.NOTNULL;
        }
        else
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
}
