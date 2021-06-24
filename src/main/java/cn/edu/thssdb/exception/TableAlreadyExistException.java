package cn.edu.thssdb.exception;

public class TableAlreadyExistException extends MyException {
    public TableAlreadyExistException() {
    }

    public TableAlreadyExistException(Object key) {
        super(key);
    }

    @Override
    public String getMessage() {
        if (key == null)
            return "Exception: table already exists!";
        return "Exception: table \"" + key + "\" already exists!";
    }
}
