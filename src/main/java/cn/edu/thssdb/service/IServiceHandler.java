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
import cn.edu.thssdb.utils.Global;
import org.apache.thrift.TException;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public class IServiceHandler implements IService.Iface {

  private long nextSessionId = 0;
  private Set<Long> abortSessions = new HashSet<>();

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

    resp.setSessionId(nextSessionId++);

    if (isValidAccount(username, password)) {
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
    // TODO
    Status status = new Status(Global.FAILURE_CODE);
    boolean isAbort = false;
    boolean hasResult = false;
    return new ExecuteStatementResp(status, isAbort, hasResult);
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
