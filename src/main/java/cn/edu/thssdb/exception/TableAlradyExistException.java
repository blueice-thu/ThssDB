package cn.edu.thssdb.exception;

public class TableAlradyExistException extends MyException {
    public TableAlradyExistException() {}

    public TableAlradyExistException(Object key) {super(key);}

    @Override
    public String getMessage() {
        if (key == null)
            return "Exception: table already exists!";
        return "Exception: table \"" + key + "\" already exists!";
    }
}
