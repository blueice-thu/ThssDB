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
        } else {
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
        try {
            if ("ALTER".equals(elements[0])) {// 添加属性
                if (numElem == 6 && elements[1].equals("TABLE") && elements[3].equals("ADD")) {
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
                }
                // 删除属性
                else if (numElem == 5 && elements[1].equals("TABLE") && elements[3].equals("DROP")) {
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
                }
                // 更改属性类型
                else if (numElem == 6 && elements[1].equals("TABLE") && elements[3].equals("ALTER")) {
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
                    status.setMsg("\"" + statement + "\" is not a command");
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
