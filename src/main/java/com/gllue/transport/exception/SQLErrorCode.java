package com.gllue.transport.exception;

/** SQL error code. */
public interface SQLErrorCode {

  /**
   * Get error code.
   *
   * @return error code
   */
  int getErrorCode();

  /**
   * Get SQL state.
   *
   * @return SQL state
   */
  String getSqlState();

  /**
   * Get error message.
   *
   * @return error message
   */
  String getErrorMessage();

  /**
   * Check the error code is equals to others.
   *
   * @return equals or not
   */
  default boolean equals(SQLErrorCode errorCode) {
    return getErrorCode() == errorCode.getErrorCode();
  }
}
