package cn.edu.thssdb.parser.statement;

public class ColumnFullName extends Comparer {
    // null="*"
    public String tableName, columnName;

    public ColumnFullName(String tableName, String columnName) {
        this.tableName = tableName;
        this.columnName = columnName;
    }

    @Override
    public Type get_type() {
        return Type.COLUMN_FULL_NAME;
    }
}
