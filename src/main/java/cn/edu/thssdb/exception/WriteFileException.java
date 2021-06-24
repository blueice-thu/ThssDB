package cn.edu.thssdb.exception;

public class WriteFileException extends MyException {
    public WriteFileException() {
        super();
    }

    public WriteFileException(Object key) {
        super(key);
    }

    @Override
    public String getMessage() {
        if (key == null)
            return "Exception: fail to write file!";
        else
            return "Exception: fail to write file \"" + key + "\"!";
    }
}
