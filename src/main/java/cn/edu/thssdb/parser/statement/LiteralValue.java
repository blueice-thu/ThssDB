package cn.edu.thssdb.parser.statement;

public class LiteralValue extends Comparer {
    public Comparable value;

    public LiteralValue(Comparable value) {
        this.value = value;
    }

    @Override
    public Type get_type() {
        return Type.LITERAL_VALUE;
    }
}