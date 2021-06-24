package cn.edu.thssdb.utils;

public class Global {
    public static final String CONNECT = "connect";
    public static final String DISCONNECT = "disconnect";
    public static final String HELP = "show help";
    public static final String USE = "use";
    public static final String QUIT = "quit";
    public static final String SHOW = "show";
    public static final String SHOW_TIME = "show time";
    public static final String HELP_TEXT = "Usage:\n" +
            "\tconnect [<username> <password>]\n" +
            "\tdisconnect\n" +
            "\tcreate database <name>\n" +
            "\tcreate table <name>\n" +
            "\tshow time        Get current time. \"Tue May 04 22:58:49 CST 2021\"\n" +
            "\tshow [tables | databases | help]\n" +
            "\tquit             Close connection and quit client.\n" +
            "\thelp             Get help information.";
    public static final String S_URL_INTERNAL = "jdbc:default:connection";
    public static int fanout = 129;
    public static int SUCCESS_CODE = 0;
    public static int FAILURE_CODE = -1;
    public static String DEFAULT_SERVER_HOST = "127.0.0.1";
    public static int DEFAULT_SERVER_PORT = 6667;
    public static String DEFAULT_USERNAME = "";
    public static String DEFAULT_PASSWORD = "";
    public static String CLI_PREFIX = "ThssDB> ";
    public static String PERSIST_PATH = "data";
    public static String PERSIST_TABLE_META_SUFFIX = ".meta";
    public static String PERSIST_TABLE_ROWS_SUFFIX = ".data";
    public static final String LOG_PATH = "log";

    public static int LOCK_TRY_TIME = 5;
    public static long LOCK_WAIT_INTERVAL = 500;
}
