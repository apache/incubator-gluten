package io.glutenproject.exception;

public class GlutenException extends RuntimeException {

  public GlutenException() {}

  public GlutenException(String message) {
    super(message);
  }

  public GlutenException(String message, Throwable cause) {
    super(message, cause);
  }

  public GlutenException(Throwable cause) {
    super(cause);
  }
}
