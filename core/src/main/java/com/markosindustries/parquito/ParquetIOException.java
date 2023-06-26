package com.markosindustries.parquito;

public class ParquetIOException extends RuntimeException {
  public ParquetIOException() {}

  public ParquetIOException(final String message) {
    super(message);
  }

  public ParquetIOException(final String message, final Throwable cause) {
    super(message, cause);
  }

  public ParquetIOException(final Throwable cause) {
    super(cause);
  }

  public ParquetIOException(
      final String message,
      final Throwable cause,
      final boolean enableSuppression,
      final boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}
