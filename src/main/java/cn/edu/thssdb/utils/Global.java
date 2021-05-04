package cn.edu.thssdb.utils;

public class Global {
  public static int fanout = 129;

  public static int SUCCESS_CODE = 0;
  public static int FAILURE_CODE = -1;

  public static String DEFAULT_SERVER_HOST = "127.0.0.1";
  public static int DEFAULT_SERVER_PORT = 6667;
  public static String DEFAULT_USERNAME = "";
  public static String DEFAULT_PASSWORD = "";

  public static String CLI_PREFIX = "ThssDB> ";

  public static final String CONNECT = "connect";
  public static final String DISCONNECT = "disconnect";
  public static final String HELP = "show help";
  public static final String QUIT = "quit";
  public static final String SHOW_TIME = "show time";

  public static final String HELP_TEXT = "Usage:\n" +
          "\tconnect <username> <password>\n" +
          "\tshow time      Get current time. \"Tue May 04 22:58:49 CST 2021\"\n" +
          "\tquit           Close connection and quit client.\n" +
          "\thelp           Get help information.";

  public static final String S_URL_INTERNAL = "jdbc:default:connection";
}
