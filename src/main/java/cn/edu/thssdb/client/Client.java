package cn.edu.thssdb.client;

import cn.edu.thssdb.rpc.thrift.*;
import cn.edu.thssdb.utils.Global;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.Scanner;

public class Client {

  private static final Logger logger = LoggerFactory.getLogger(Client.class);

  static final String HOST_ARGS = "h";
  static final String HOST_NAME = "host";

  static final String HELP_ARGS = "help";
  static final String HELP_NAME = "help";

  static final String PORT_ARGS = "p";
  static final String PORT_NAME = "port";

  private static final PrintStream SCREEN_PRINTER = new PrintStream(System.out);
  private static final Scanner SCANNER = new Scanner(System.in);

  private static TTransport transport;
  private static TProtocol protocol;
  private static IService.Client client;
  private static CommandLine commandLine;

  private static long sessionId = -1;

  public static void main(String[] args) {
    commandLine = parseCmd(args);
    if (commandLine.hasOption(HELP_ARGS)) {
      showHelp();
      return;
    }
    try {
      echoStarting();
      String host = commandLine.getOptionValue(HOST_ARGS, Global.DEFAULT_SERVER_HOST);
      int port = Integer.parseInt(commandLine.getOptionValue(PORT_ARGS, String.valueOf(Global.DEFAULT_SERVER_PORT)));
      transport = new TSocket(host, port);
      transport.open();
      protocol = new TBinaryProtocol(transport);
      client = new IService.Client(protocol);
      boolean open = true;
      do {
        print(Global.CLI_PREFIX);

        // Use ";" to cut several statements
        String[] msgList = SCANNER.nextLine().trim().split(";");

        long startTime = System.currentTimeMillis();
        for (String msg : msgList) {
          switch (msg.trim()) {
            // TODO: Add more commands here
            case "":
              break;
            case Global.DISCONNECT:
              disconnect();
              break;
            case Global.HELP:
              showHelp();
              break;
            case Global.QUIT:
              open = false;
              break;
            case Global.SHOW_TIME:
              getTime();
              break;
            default:
              // Need more arguments
              String[] elements = msg.split(" ");
              int numElem = elements.length;
              switch (elements[0]) {
                case Global.CONNECT:
                  if (numElem == 1) {
                    connect("", "");
                  } else if (numElem == 3) {
                    connect(elements[1], elements[2]);
                  } else {
                    showInvalid();
                  }
                  break;
                case Global.SHOW:
                  if (numElem == 2) {
                    showSomething(elements[1]);
                  }
                  else {
                    showInvalid();
                  }
                  break;
                default:
                  executeStatement(msg);
                  break;
              }
          }
        }

        long endTime = System.currentTimeMillis();
        println("It costs " + (endTime - startTime) + " ms.");

      } while (open);
      transport.close();
    } catch (TTransportException e) {
      logger.error(e.getMessage());
    }
  }

  private static void getTime() {
    GetTimeReq req = new GetTimeReq();
    try {
      println(client.getTime(req).getTime());
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void connect(String username, String password) {
    ConnectReq req = new ConnectReq(username, password);
    try {
      ConnectResp resp = client.connect(req);
      if (resp.getStatus().getCode() == Global.SUCCESS_CODE) {
        sessionId = resp.getSessionId();
      }
      else {
        println("Connect failed:" + resp.getStatus().getMsg());
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void disconnect() {
    DisconnetReq req = new DisconnetReq();
    try {
      DisconnetResp resp = client.disconnect(req);
      if (resp.getStatus().getCode() == Global.SUCCESS_CODE) {
        sessionId = -1;
      }
      else {
        println("Disconnect failed:" + resp.getStatus().getMsg());
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }
  }

  private static void executeStatement(String statement) {
    // TODO
    ExecuteStatementReq req = new ExecuteStatementReq();
    req.setStatement(statement);
    req.setSessionId(sessionId);
    try {
      ExecuteStatementResp resp = client.executeStatement(req);
      if (resp.getStatus().getCode() == Global.SUCCESS_CODE) {
        println(resp.getStatus().toString());
      }
      else {
        println("Execute statement \"" + statement + "\" failed: " + resp.getStatus().getMsg());
      }
    } catch (TException e) {
      logger.error(e.getMessage());
    }

  }

  private static void showSomething(String item) {
    ShowReq req = new ShowReq();
    req.setSessionId(sessionId);
    req.setItem(item);
    try {
      ShowResp resp = client.show(req);
      if (resp.getStatus().getCode() == Global.SUCCESS_CODE) {
        println(resp.getContents());
      }
      else {
        println(resp.getStatus().toString());
      }
    } catch (TException e) {
      e.printStackTrace();
    }
  }

  static Options createOptions() {
    Options options = new Options();
    options.addOption(Option.builder(HELP_ARGS)
        .argName(HELP_NAME)
        .desc("Display help information(optional)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(HOST_ARGS)
        .argName(HOST_NAME)
        .desc("Host (optional, default 127.0.0.1)")
        .hasArg(false)
        .required(false)
        .build()
    );
    options.addOption(Option.builder(PORT_ARGS)
        .argName(PORT_NAME)
        .desc("Port (optional, default 6667)")
        .hasArg(false)
        .required(false)
        .build()
    );
    return options;
  }

  static CommandLine parseCmd(String[] args) {
    Options options = createOptions();
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      logger.error(e.getMessage());
      println("Invalid command line argument!");
      System.exit(-1);
    }
    return cmd;
  }

  static void showInvalid() {
    println("Invalid statements!");
    showHelp();
  }

  static void showHelp() {
    println(Global.HELP_TEXT);
  }

  static void echoStarting() {
    println("----------------------");
    println("Starting ThssDB Client");
    println("----------------------");
  }

  static void print(String msg) {
    SCREEN_PRINTER.print(msg);
  }

  static void println() {
    SCREEN_PRINTER.println();
  }

  static void println(String msg) {
    SCREEN_PRINTER.println(msg);
  }
}
