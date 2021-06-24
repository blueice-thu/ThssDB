package cn.edu.thssdb.exception;

public class TableNotExistException extends MyException {
    public TableNotExistException() {
    }

    public TableNotExistException(Object key) {
        super(key);
    }

    @Override
    public String getMessage() {
        if (key == null)
            return "Exception: table doesn't exists!";
        return "Exception: table \"" + key + "\" doesn't exists!";
    }
}
