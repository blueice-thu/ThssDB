package cn.edu.thssdb.parser.statement;

public abstract class Comparer {
    public abstract Type get_type();

    public enum Type {
        COLUMN_FULL_NAME, LITERAL_VALUE;
    }
}