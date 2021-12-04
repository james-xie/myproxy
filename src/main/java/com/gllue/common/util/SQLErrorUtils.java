package com.gllue.common.util;

import com.gllue.transport.backend.BackendResultReadException;
import com.gllue.transport.exception.MySQLServerErrorCode;
import com.gllue.transport.exception.SQLErrorCode;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SQLErrorUtils {
  public static final int BAD_ERROR_CODE = -123456;

  public static SQLErrorCode tryGetSQLErrorCode(Throwable e) {
    if (e instanceof BackendResultReadException) {
      return ((BackendResultReadException) e).getErrorCode();
    }
    return null;
  }

  public static int getErrorCode(SQLErrorCode errorCode) {
    return errorCode == null ? BAD_ERROR_CODE : errorCode.getErrorCode();
  }

  public static boolean checkForExceptionMatchingErrorCode(Throwable e, SQLErrorCode code) {
    return code.getErrorCode() == getErrorCode(tryGetSQLErrorCode(e));
  }

  public static boolean isTableAlreadyExists(Throwable e) {
    return checkForExceptionMatchingErrorCode(e, MySQLServerErrorCode.ER_TABLE_EXISTS_ERROR);
  }
}
