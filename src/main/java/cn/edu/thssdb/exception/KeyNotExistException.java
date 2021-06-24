package cn.edu.thssdb.exception;

public class KeyNotExistException extends MyException {

  public KeyNotExistException() {
    super();
  }

  public KeyNotExistException(Object key) {
    super(key);
  }

  @Override
  public String getMessage() {
    if (key == null)
      return "Exception: key doesn't exist!";
    else
      return "Exception: key \"" + key + "\" doesn't exist!";
  }
}
