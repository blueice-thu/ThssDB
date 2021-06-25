package cn.edu.thssdb.exception;

public class LockWaitTimeoutException extends MyException {
    public LockWaitTimeoutException() {
        super();
    }

    public LockWaitTimeoutException(Object key) {
        super(key);
    }

    @Override
    public String getMessage() {
        if (key == null)
            return "Exception: wait lock timeout!";
        return "Exception: wait lock of \"" + key + "\" timeout!";
    }
}
