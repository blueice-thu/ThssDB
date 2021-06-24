package cn.edu.thssdb.exception;

public abstract class MyException extends RuntimeException {
    protected String key;

    public MyException() {
    }

    public MyException(Object key) {
        try {
            this.key = String.valueOf(key);
        } catch (Exception e) {
            this.key = null;
        }
    }

    public abstract String getMessage();
}
