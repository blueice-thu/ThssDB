package cn.edu.thssdb.service;

import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnetReq;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import java.util.*;

public class IServiceHandler implements IService.Iface {

  private long nextSessionId = 0;
  private Manager databaseManager = new Manager();
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

    switch (statement) {
      case Global.SHOW_TABLES: {
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
        resp.setTables(joiner.toString());
        break;
      }
      case Global.SHOW_DATABASES: {
        // TODO
        StringJoiner joiner = new StringJoiner("\n");

        break;
      }
      default:
        status.setCode(Global.FAILURE_CODE);
        status.setMsg("Unknown command: \"" + statement + "\"");
        resp.setIsAbort(false);
        resp.setHasResult(false);
        return resp;
    }

    // TODO
    status.setCode(Global.FAILURE_CODE);
    status.setMsg("\"" + statement +"\" is not a command");

    resp.setIsAbort(isAbort);
    resp.setHasResult(hasResult);

    return resp;
  }

  private boolean isValidAccount(String username, String password) {
    if (username.equals(Global.DEFAULT_USERNAME) && password.equals(Global.DEFAULT_PASSWORD)) {
      return true;
    }
    return false;
  }

  private boolean isValidSessionId(long sessionId) {
    return sessionId >= 0 && sessionId < nextSessionId && !abortSessions.contains(sessionId);
  }
}
