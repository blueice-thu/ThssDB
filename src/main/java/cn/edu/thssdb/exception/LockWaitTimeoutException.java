package cn.edu.thssdb.exception;

public class LockWaitTimeoutException extends MyException {
    public LockWaitTimeoutException() {super();}
    public LockWaitTimeoutException(Object key) {super(key);}

    @Override
    public String getMessage() {
        return "Exception: wait lock timeout!";
    }
}
