package cn.edu.thssdb.schema;

import cn.edu.thssdb.type.ColumnType;

import java.io.Serializable;

public class Column implements Comparable<Column>, Serializable {
    private static final long serialVersionUID = -5809781518272943999L;

    private String name;
    private ColumnType type;
    private int primary;
    private boolean notNull;
    private int maxLength;

    public Column(String name, ColumnType type, int primary, boolean notNull, int maxLength) {
        this.name = name;
        this.type = type;
        this.primary = primary;
        this.notNull = notNull;
        this.maxLength = maxLength;
    }

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }

    public void setType(ColumnType type) {
        this.type = type;
    }

    public int getPrimary() {
        return primary;
    }

    public boolean isNotNull() {
        return notNull;
    }

    public int getMaxLength() {
        return maxLength;
    }

    public String toString() {
        return String.format("%-8s | %-8s | %-8s | %-8s | %-8s", name, type, primary, notNull, maxLength);
    }

    public boolean isPrimary() {
        return primary != 0;
    }

    public void setPrimary() {
        this.primary = 1;
    }

    @Override
    public int compareTo(Column e) {
        return name.compareTo(e.name);
    }
}
