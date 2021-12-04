package com.gllue.transport.exception;


import com.google.common.base.Preconditions;
import java.util.HashSet;
import java.util.Set;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Custom error code for the proxy server.
 * Caution please:
 *  All the custom error code must be greater than or equals 10000.
 *
 */
@Getter
public enum ServerErrorCode implements SQLErrorCode {

  ER_LOST_BACKEND_CONNECTION(10001, "Lost backend connection."),

  ER_TOO_MANY_BACKEND_CONNECTION(10002, "Too many backend connections. [%s]"),

  ER_TOO_MANY_EXECUTION_TASK(10010, "Too many execution tasks."),


  ER_ILLEGAL_ARGUMENT(10100, "Illegal argument. [%s]"),

  ER_BAD_CONFIGURATION(10101, "Bad configuration."),

  ER_BAD_SQL(10102, "Bad SQL. [%s]"),

  ER_MISSING_COMMENT_ATTRIBUTE(10103, "Missing comment attribute. [%s]"),

  ER_BAD_COMMENT_ATTRIBUTE(10104, "Bad comment attribute. [key=%s, value=%s]"),

  ER_SERVER_ERROR(10000, "Server error. [%s]");

  private final int errorCode;

  private final String errorMessage;

  ServerErrorCode(final int errorCode, final String errorMessage) {
    Preconditions.checkArgument(errorCode >= 10000 && errorCode < 100000);

    this.errorCode = errorCode;
    this.errorMessage = errorMessage;
  }

  @Override
  public String getSqlState() {
    return "s" + errorCode;
  }
}
