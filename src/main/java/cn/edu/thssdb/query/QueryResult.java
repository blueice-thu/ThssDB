package cn.edu.thssdb.query;

import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.utils.Cell;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class QueryResult {

  private List<MetaInfo> metaInfoInfos;
  private List<Integer> index;
  private List<Cell> attrs;
  private String msg;

  public QueryResult(QueryTable[] queryTables) {
    // TODO
    this.index = new ArrayList<>();
    this.attrs = new ArrayList<>();
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
}
