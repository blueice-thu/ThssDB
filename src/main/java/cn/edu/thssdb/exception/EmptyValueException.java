package cn.edu.thssdb.exception;

public class EmptyValueException extends MyException {
    public EmptyValueException() {}

    public EmptyValueException(Object key) {super(key);}

    @Override
    public String getMessage() {
        if (key == null)
            return "Exception: empty value!";
        return "Exception: empty value of \"" + key + "\"!";
    }
}
