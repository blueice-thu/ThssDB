package cn.edu.thssdb.exception;

public class DatabaseNotExistException extends MyException {
    public DatabaseNotExistException() {
        super();
    }

    public DatabaseNotExistException(Object key) {
        super(key);
    }

    @Override
    public String getMessage() {
        if (key == null)
            return "Exception: database doesn't exist!";
        else
            return "Exception: database \"" + key + "\" doesn't exist!";
    }
}
