package cn.edu.thssdb.service;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import java.util.*;

public class IServiceHandler implements IService.Iface {

  private long nextSessionId = 0;
  private Manager databaseManager = Manager.getInstance();
  private Set<Long> abortSessions = new HashSet<>();
  private HashMap<Long, Session> sessions = new HashMap<>();

  @Override
  public GetTimeResp getTime(GetTimeReq req) throws TException {
    GetTimeResp resp = new GetTimeResp();
    resp.setTime(new Date().toString());
    resp.setStatus(new Status(Global.SUCCESS_CODE));
    return resp;
  }

  @Override
  public ConnectResp connect(ConnectReq req) throws TException {
    // TODO
    ConnectResp resp = new ConnectResp();
    String username = req.getUsername();
    String password = req.getPassword();

    if (isValidAccount(username, password)) {
      sessions.put(nextSessionId, new Session(nextSessionId));
      resp.setSessionId(nextSessionId++);
      resp.setStatus(new Status(Global.SUCCESS_CODE));
    }
    else {
      resp.setStatus(new Status(Global.FAILURE_CODE));
    }

    return resp;
  }

  @Override
  public DisconnetResp disconnect(DisconnetReq req) throws TException {
    // TODO
    long sessionId = req.getSessionId();
    Status status = new Status();
    if (isValidSessionId(sessionId)) {
      status.setCode(Global.SUCCESS_CODE);
      abortSessions.add(sessionId);
      sessions.remove(sessionId);
    }
    else {
      status.setCode(Global.FAILURE_CODE);
      status.setMsg("Invalid sessionId");
    }
    DisconnetResp resp = new DisconnetResp();
    resp.setStatus(status);
    return resp;
  }

  @Override
  public ShowResp show(ShowReq req) throws TException {
    long sessionId = req.getSessionId();
    String item = req.getItem();

    ShowResp resp = new ShowResp();
    Status status = new Status();
    resp.setStatus(status);

    if (!isValidSessionId(sessionId)) {
      status.setCode(Global.FAILURE_CODE);
      status.setMsg("Invalid sessionId.");
      return resp;
    }

    Session session = sessions.get(sessionId);

    switch (item) {
      case "tables": {
        if (session.getCurrentDatabase() == null) {
          status.setCode(Global.FAILURE_CODE);
          status.setMsg("No database selected");
          break;
        }
        StringJoiner joiner = new StringJoiner("\n");
        ArrayList<String> tableNames = session.getCurrentDatabase().getTableNameList();
        for (String tableName : tableNames)
          joiner.add(tableName);
        status.setCode(Global.SUCCESS_CODE);
        resp.setContents(joiner.toString());
        break;
      }
      case "databases": {
        StringJoiner joiner = new StringJoiner("\n");
        ArrayList<String> databaseNames = databaseManager.getDatabaseNameList();
        for (String databaseName : databaseNames)
          joiner.add(databaseName);
        status.setCode(Global.SUCCESS_CODE);
        resp.setContents(joiner.toString());
        break;
      }
      case "help": {
        status.setCode(Global.SUCCESS_CODE);
        resp.setContents(Global.HELP_TEXT);
        break;
      }
      default:
        status.setCode(Global.FAILURE_CODE);
        status.setMsg("No such show items: " + item);
    }
    return resp;
  }

  @Override
  public ExecuteStatementResp executeStatement(ExecuteStatementReq req) throws TException {
    String statement = req.getStatement();
    long sessionId = req.getSessionId();

    ExecuteStatementResp resp = new ExecuteStatementResp();
    Status status = new Status();
    resp.setStatus(status);
    boolean isAbort = false;
    boolean hasResult = false;

    if (!isValidSessionId(sessionId)) {
      status.setCode(Global.FAILURE_CODE);
      status.setMsg("Invalid sessionId.");
      resp.setIsAbort(false);
      resp.setHasResult(false);
      return resp;
    }

    Session session = sessions.get(sessionId);

    String[] elements = statement.split(" ");
    int numElem = elements.length;
    switch (elements[0]) {
      case "use": {
        if (numElem == 2) {
          if (databaseManager.hasDatabase(elements[1])) {
            session.setCurrentDatabase(databaseManager.getDatabase(elements[1]));
            status.setCode(Global.SUCCESS_CODE);
          }
          else {
            status.setCode(Global.FAILURE_CODE);
            status.setMsg("No such database: " + elements[1]);
          }
        } else {
          status.setCode(Global.FAILURE_CODE);
          status.setMsg("Unknown command: \"" + statement + "\"");
        }
      }
      break;
      case "create": {
        if (numElem == 3 && elements[1].equals("database")) {
          // create database
          if (databaseManager.hasDatabase(elements[2])) {
            status.setCode(Global.FAILURE_CODE);
            status.setMsg("Database has existed: " + elements[2]);
            break;
          }
          if (databaseManager.createDatabaseIfNotExists(elements[2])) {
            status.setCode(Global.SUCCESS_CODE);
          }
          else {
            status.setCode(Global.FAILURE_CODE);
            status.setMsg("Fail to create database: " + elements[2]);
          }
        } else if (numElem >= 3 && elements[1].equals("table")) {
          // create table
          if (session.getCurrentDatabase() == null) {
            status.setCode(Global.FAILURE_CODE);
            status.setMsg("No database selected");
            break;
          }
          String tableName = elements[2];
          Database database = session.getCurrentDatabase();
          if (database.hasTable(tableName)) {
            status.setCode(Global.FAILURE_CODE);
            status.setMsg("Table already exists: " + tableName);
            break;
          }
          // TODO
          if (database.create(tableName, null)) {
            status.setCode(Global.SUCCESS_CODE);
          }
          else {
            status.setCode(Global.FAILURE_CODE);
            status.setMsg("Fail to create table: " + tableName);
          }
        } else {
          status.setCode(Global.FAILURE_CODE);
          status.setMsg("Unknown command: \"" + statement + "\"");
        }
      }
      break;
      case "drop": {
        if (numElem == 3 && elements[1].equals("database")) {
          if (!databaseManager.hasDatabase(elements[2])) {
            status.setCode(Global.FAILURE_CODE);
            status.setMsg("No such database: " + elements[2]);
            break;
          }
          if (databaseManager.deleteDatabase(elements[2])) {
            status.setCode(Global.SUCCESS_CODE);
          }
          else {
            status.setCode(Global.FAILURE_CODE);
            status.setMsg("Fail to drop database: " + elements[2]);
          }
        } else if (numElem == 3 && elements[1].equals("table")) {
          // drop table
          if (session.getCurrentDatabase() == null) {
            status.setCode(Global.FAILURE_CODE);
            status.setMsg("No database selected");
            break;
          }
          String tableName = elements[2];
          Database database = session.getCurrentDatabase();
          if (!database.hasTable(tableName)) {
            status.setCode(Global.FAILURE_CODE);
            status.setMsg("No such table: " + tableName);
            break;
          }
          if (database.drop(tableName)) {
            status.setCode(Global.SUCCESS_CODE);
          }
          else {
            status.setCode(Global.FAILURE_CODE);
            status.setMsg("Fail to drop table: " + tableName);
          }
        } else {
          status.setCode(Global.FAILURE_CODE);
          status.setMsg("Unknown command: \"" + statement + "\"");
          return resp;
        }
      }
      break;
      default: {
        status.setCode(Global.FAILURE_CODE);
        status.setMsg("\"" + statement +"\" is not a command");
      }
    }

    // TODO
    resp.setIsAbort(isAbort);
    resp.setHasResult(hasResult);

    return resp;
  }

  private boolean isValidAccount(String username, String password) {
    return username.equals(Global.DEFAULT_USERNAME) && password.equals(Global.DEFAULT_PASSWORD);
  }

  private boolean isValidSessionId(long sessionId) {
    return sessionId >= 0 && sessionId < nextSessionId && !abortSessions.contains(sessionId);
  }
}
