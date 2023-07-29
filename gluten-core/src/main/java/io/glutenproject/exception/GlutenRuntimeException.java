package io.glutenproject.exception;

public class GlutenRuntimeException extends RuntimeException {

  public GlutenRuntimeException() {}

  public GlutenRuntimeException(String message) {
    super(message);
  }

  public GlutenRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public GlutenRuntimeException(Throwable cause) {
    super(cause);
  }
}
