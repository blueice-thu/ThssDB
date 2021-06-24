package cn.edu.thssdb.exception;

public class DatabaseAlreadyExistException extends MyException {
    public DatabaseAlreadyExistException() {
    }

    public DatabaseAlreadyExistException(Object key) {
        super(key);
    }

    @Override
    public String getMessage() {
        if (key == null)
            return "Exception: database already exists!";
        return "Exception: database \"" + key + "\" already exists!";
    }
}
