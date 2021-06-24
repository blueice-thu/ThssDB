package cn.edu.thssdb.exception;

public class UnknownTypeException extends MyException {
    public UnknownTypeException() {super();}
    public UnknownTypeException(Object key) {super(key);}
    @Override
    public String getMessage() {
        if (key == null)
            return "Exception: unknown type!";
        return "Exception: unknown type \"" + key + "\"!";
    }
}
