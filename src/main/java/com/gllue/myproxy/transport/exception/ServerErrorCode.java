package com.gllue.myproxy.transport.exception;

import com.google.common.base.Preconditions;
import lombok.Getter;

/**
 * Custom error code for the proxy server. Caution please: All the custom error code must be greater
 * than or equals 10000.
 */
@Getter
public enum ServerErrorCode implements SQLErrorCode {
  ER_LOST_BACKEND_CONNECTION(50001, "Lost backend connection."),

  ER_TOO_MANY_BACKEND_CONNECTION(50002, "Too many backend connections. [%s]"),

  ER_TOO_MANY_EXECUTION_TASK(50010, "Too many execution tasks."),

  ER_ILLEGAL_ARGUMENT(50100, "Illegal argument. [%s]"),

  ER_BAD_CONFIGURATION(50101, "Bad configuration."),

  ER_BAD_SQL(50102, "Bad SQL. [%s]"),

  ER_MISSING_COMMENT_ATTRIBUTE(50103, "Missing comment attribute. [%s]"),

  ER_BAD_COMMENT_ATTRIBUTE(50104, "Bad comment attribute. [key=%s, value=%s]"),

  ER_NO_ENCRYPT_KEY(50105, "No encryption key."),

  ER_BAD_ENCRYPT_KEY(
      50106, "Bad encryption key [%s], correct encryption key format: '<database>:<encryptKey>'"),

  ER_QUERY_RESULT_OUT_OF_MEMORY(50107, "Query result buffer out of memory. [size: %sKB]"),

  ER_SERVER_ERROR(50000, "Server error. [%s]");

  private final int errorCode;

  private final String errorMessage;

  ServerErrorCode(final int errorCode, final String errorMessage) {
    Preconditions.checkArgument(errorCode >= 50000 && errorCode < 52000);

    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  @Override
  public String getSqlState() {
    return "s5000";
  }
}
