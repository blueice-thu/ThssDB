package cn.edu.thssdb.exception;

public class DuplicateKeyException extends MyException {

    public DuplicateKeyException() {
        super();
    }

    public DuplicateKeyException(Object key) {
        super(key);
    }

    @Override
    public String getMessage() {
        if (key == null)
            return "Exception: insertion caused duplicated keys!";
        else
            return "Exception: insertion \"" + key + "\" caused duplicated keys!";
    }
}
