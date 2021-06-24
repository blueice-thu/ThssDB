package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.statement.*;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Cell;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class QueryResult {

    private List<MetaInfo> metaInfoInfos;
    private List<Integer> index;
    private List<Cell> attrs;
    private List<Table> tables;
    private String msg;

    public QueryResult() {
    }

    public QueryResult(QueryTable[] queryTables) {
        // TODO
        this.index = new ArrayList<>();
        this.attrs = new ArrayList<>();
    }

    public QueryResult(ArrayList<Table> tables) {

    }

    public QueryResult(Table table) {
        this.tables = new ArrayList<Table>();
        this.tables.add(table);
        this.metaInfoInfos = new ArrayList<MetaInfo>();
        this.metaInfoInfos.add(new MetaInfo(table.getTableName(), table.columns));
    }

    public static Row combineRow(LinkedList<Row> rows) {
        // TODO
        return null;
    }

    public Row generateQueryRecord(Row row) {
        // TODO
        return null;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public ArrayList<Row> getRowFromQuery(Condition condition) throws Exception {
        return filterRows(condition);
    }

    // TODO: get return string from selected rows
    public String selectQuery(Condition condition) throws Exception {
        ArrayList<Row> selectedRows = filterRows(condition);
        // TODO: get return string from selected rows
        return null;
    }

    private ArrayList<Row> filterRows(Condition conditions) throws Exception {
        ArrayList<Row> rows = new ArrayList<>();
        if (tables.size() == 1) {
            Table onlyTable = tables.get(0);
            onlyTable.readLock();
            for (Row row : onlyTable) {
                if (conditions == null || satisfyConditions(row, conditions)) {
                    rows.add(row);
                }
            }
            onlyTable.readUnlock();
        }
        // TODO: 查询时有多个表的情况
//    else if (tables.size() == 2) {
//      Table tableLeft = tables.get(0);
//      Table tableRight = tables.get(1);
//      tableLeft.readLock();
//      tableRight.readLock();
//      while (tableLeft.hasNext()) {
//        Row rowLeft = queryTableLeft.next();
//        queryTableRight.refresh();
//        while (queryTableRight.hasNext()) {
//          Row rowCombined = combineRow(rowLeft, queryTableRight.next());
//          if (condition == null || calcCondition(condition, metaInfos.get(0), metaInfos.get(1), rowCombined)) {
//            combinedRowList.add(rowCombined);
//          }
//        }
//      }
//      queryTableLeft.readUnlock();
//      queryTableRight.readUnlock();
//    }
        return rows;
    }

    private boolean satisfyConditions(Row row, Condition condition) throws Exception {
        String op = condition.op;
        Comparable valueLeft = getExpressionValue(condition.expressionLeft, metaInfoInfos.get(0), row); // attrName
        Comparable valueRight = getExpressionValue(condition.expressionRight, metaInfoInfos.size() > 1 ? metaInfoInfos.get(1) : null, row); // attrValue

        // 字符比较或者数字比较
        int compareResult;
        if (valueLeft instanceof String) {
            compareResult = valueLeft.toString().compareTo(valueRight.toString());
        } else {
            compareResult = Double.valueOf(valueLeft.toString()).compareTo(Double.valueOf(valueRight.toString()));
        }

        switch (op) {
            case "=":
                return compareResult == 0;
            case "<>":
                return compareResult != 0;
            case "<":
                return compareResult < 0;
            case ">":
                return compareResult > 0;
            case "<=":
                return compareResult <= 0;
            case ">=":
                return compareResult >= 0;
            default:
                return false;
        }
    }

    // TODO: 还不支持nested表达式
    Comparable getExpressionValue(Expression expression, MetaInfo metainfo, Row row) throws Exception {
        if (expression.op != null) {
            throw new Exception("nested expression not supported yet");
        }
        Comparer comparer = expression.comparerLeft;

        if (comparer.get_type().equals(Comparer.Type.COLUMN_FULL_NAME)) {
            ColumnFullName fullName = (ColumnFullName) comparer;
            int index = metainfo.columnFind(fullName.columnName);
            if (index == -1) {
                throw new Exception("Column does not exist.");
            }
            return row.getEntries().get(index).value;
        } else {
            return ((LiteralValue) comparer).value;
        }
    }

}
