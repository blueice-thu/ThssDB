package cn.edu.thssdb.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public class Row implements Serializable {
    private static final long serialVersionUID = -5809782578272943999L;
    protected ArrayList<Entry> entries = new ArrayList<>();
    ;

    public Row() {

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
        if (entries == null)
            return "EMPTY";
        StringJoiner sj = new StringJoiner(", ");
        for (Entry e : entries)
            sj.add(e.toString());
        return sj.toString();
    }
}
