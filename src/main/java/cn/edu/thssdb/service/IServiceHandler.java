package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLLexer;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.parser.SQLVisitorImple;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.utils.Global;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.thrift.TException;

import java.util.*;

public class IServiceHandler implements IService.Iface {

    private long nextSessionId = 1;
    private Manager databaseManager = Manager.getInstance();
    private Set<Long> abortSessions = new HashSet<>();
    private HashMap<Long, Session> sessions = new HashMap<>();

    @Override
    public GetTimeResp getTime(GetTimeReq req) {
        GetTimeResp resp = new GetTimeResp();
        resp.setTime(new Date().toString());
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        return resp;
    }

    @Override
    public ConnectResp connect(ConnectReq req) {
        ConnectResp resp = new ConnectResp();
        String username = req.getUsername();
        String password = req.getPassword();

        if (isValidAccount(username, password)) {
            sessions.put(nextSessionId, new Session(nextSessionId));
            resp.setSessionId(nextSessionId++);
            resp.setStatus(new Status(Global.SUCCESS_CODE));
        } else {
            resp.setStatus(new Status(Global.FAILURE_CODE));
        }

        return resp;
    }

    @Override
    public DisconnetResp disconnect(DisconnetReq req) {
        long sessionId = req.getSessionId();
        Status status = new Status();
        if (isValidSessionId(sessionId)) {
            status.setCode(Global.SUCCESS_CODE);
            abortSessions.add(sessionId);
            sessions.remove(sessionId);
        } else {
            status.setCode(Global.FAILURE_CODE);
            if (abortSessions.contains(sessionId))
                status.setMsg("SessionId has been aborted!");
            else if (sessionId < 0)
                status.setMsg("Unconnected!");
            else
                status.setMsg("Invalid sessionId!");
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

        String[] elements = statement.split(" ");
        int numElem = elements.length;
        try {
            if ("ALTER".equalsIgnoreCase(elements[0])) {// 添加属性
                if (numElem == 6 && elements[1].equalsIgnoreCase("TABLE") && elements[3].equalsIgnoreCase("ADD")) {
                    if (session.getCurrentDatabase() == null) {
                        throw new Exception("No database selected");
                    }
                    Database database = session.getCurrentDatabase();
                    String tableName = elements[2];
                    Table table = database.getTable(tableName);
                    if (table == null) {
                        throw new Exception("No such table: " + tableName);
                    }
                    table.addColumn(elements[4], elements[5], 100);
                    database.quit();
                    status.setMsg("Alter add succeed.");
                }
                // 删除属性
                else if (numElem == 5 && elements[1].equalsIgnoreCase("TABLE") && elements[3].equalsIgnoreCase("DROP")) {
                    if (session.getCurrentDatabase() == null) {
                        throw new Exception("No database selected");
                    }
                    Database database = session.getCurrentDatabase();
                    String tableName = elements[2];
                    Table table = database.getTable(tableName);
                    if (table == null) {
                        throw new Exception("No such table: " + tableName);
                    }
                    table.dropColumn(elements[4]);
                    database.quit();
                    status.setMsg("Alter drop succeed.");
                }
                // 更改属性类型
                else if (numElem == 6 && elements[1].equalsIgnoreCase("TABLE") && elements[3].equalsIgnoreCase("ALTER")) {
                    if (session.getCurrentDatabase() == null) {
                        throw new Exception("No database selected");
                    }
                    Database database = session.getCurrentDatabase();
                    String tableName = elements[2];
                    Table table = database.getTable(tableName);
                    if (table == null) {
                        throw new Exception("No such table: " + tableName);
                    }
                    table.alterColumn(elements[4], elements[5], 100);
                    database.quit();
                    status.setMsg("Alter alter column succeed.");
                }
            } else {
                SQLVisitorImple visitor = new SQLVisitorImple(databaseManager, session);
                try {
                    SQLLexer lexer = new SQLLexer(CharStreams.fromString(statement));
                    CommonTokenStream tokens = new CommonTokenStream(lexer);
                    SQLParser parser = new SQLParser(tokens);
                    QueryResult result = visitor.visitParse(parser.parse());
                    status.setMsg(result.getMsg());
                } catch (Exception e) {
                    status.setCode(Global.FAILURE_CODE);
                    status.setMsg(e.getMessage());
                }
            }
        } catch (Exception e) {
            status.setCode(Global.FAILURE_CODE);
            status.setMsg(e.getMessage());
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
        return sessionId >= 1 && sessionId < nextSessionId && !abortSessions.contains(sessionId);
    }
}
