package cn.edu.thssdb.exception;

public class ReadFileException extends MyException {
    public ReadFileException() {
        super();
    }

    public ReadFileException(Object key) {
        super(key);
    }

    @Override
    public String getMessage() {
        if (key == null)
            return "Exception: fail to read file!";
        else
            return "Exception: fail to read file \"" + key + "\"!";
    }
}
