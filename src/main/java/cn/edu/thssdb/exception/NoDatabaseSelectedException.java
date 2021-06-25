package cn.edu.thssdb.exception;

public class NoDatabaseSelectedException extends MyException {
    public NoDatabaseSelectedException() {
        super();
    }

    @Override
    public String getMessage() {
        return "No database selected!";
    }

}
