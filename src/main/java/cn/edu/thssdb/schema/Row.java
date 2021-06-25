package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class Row implements Serializable {
    private static final long serialVersionUID = -5809782578272943999L;
    protected ArrayList<Entry> entries = new ArrayList<>();
    ;

    public Row() {

    }

    public Row(String rowStr, ArrayList<Column> columnsList) throws Exception {
        String[] attrListStr = rowStr.split(",");
        if (attrListStr.length != columnsList.size()) {
            throw new Exception("ColumnValueSizeNotMatchedException");
        }
        this.entries = new ArrayList<>();
        for (int i = 0; i < attrListStr.length; i++) {
            switch (columnsList.get(i).getType()) {
                case INT:
                case LONG:
                    this.entries.add(new Entry(Long.valueOf(attrListStr[i])));
                    break;
                case FLOAT:
                case DOUBLE:
                    this.entries.add(new Entry(Double.valueOf(attrListStr[i])));
                    break;
                case STRING:
                    this.entries.add(new Entry(attrListStr[i]));
                    break;
                default:
                    break;
            }
        }
    }

    public Row(Entry[] entries) {
        this.entries = new ArrayList<>(Arrays.asList(entries));
    }

    public Row(ArrayList<Entry> entries1) {
        if (entries1 != null) {
            entries.addAll(entries1);
        }
    }

    public ArrayList<Entry> getEntries() {
        return entries;
    }

    public void appendEntries(ArrayList<Entry> entries) {
        this.entries.addAll(entries);
    }

    public String toString() {
//        if (entries == null)
//            return "EMPTY";
//        StringJoiner sj = new StringJoiner(",");
//        for (Entry e : entries)
//            sj.add(e.toString());
//        return sj.toString();
        ArrayList<String> s = new ArrayList<>();
        for (Entry e : entries)
            s.add(e.toString());
        return String.join(",", s);
    }
}
