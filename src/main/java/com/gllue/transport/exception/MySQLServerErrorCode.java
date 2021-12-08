package com.gllue.transport.exception;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Server error code for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html">Server
 *     Error Message Reference</a>
 */
@RequiredArgsConstructor
@Getter
public enum MySQLServerErrorCode implements SQLErrorCode {
  ER_DB_CREATE_EXISTS(1007, "HY000", "Can't create database '%s'; database exists"),

  ER_HANDSHAKE_ERROR(1043, "42000", "Bad handshake"),

  ER_UNKNOWN_COM_ERROR(1047, "08S01", "Unknown command"),

  ER_DBACCESS_DENIED_ERROR(1044, "42000", "Access denied for user '%s'@'%s' to database '%s'"),

  ER_ACCESS_DENIED_ERROR(1045, "28000", "Access denied for user '%s'@'%s' (using password: %s)"),

  ER_NO_DB_ERROR(1046, "3D000", "No database selected"),

  ER_BAD_DB_ERROR(1049, "42000", "Unknown database '%s'"),

  ER_TABLE_EXISTS_ERROR(1050, "42S01", "Table '%s' already exists"),

  ER_BAD_TABLE_ERROR(1051, "42S02", "Unknown table '%s'"),

  ER_NON_UNIQ_ERROR(1052, "23000", "Column '%s' in %s is ambiguous"),

  ER_BAD_FIELD_ERROR(1054, "42S22", "Unknown column '%s' in '%s'"),

  ER_DUP_FIELDNAME(1060, "42S21", "Duplicate column name '%s'"),

  ER_DUP_KEYNAME(1061, "42000", "Duplicate key name '%s'"),

  ER_PARSE_ERROR(1064, "42000", "SQL parse error: %s"),

  ER_ER_NONUNIQ_TABLE(1066, "42000", "Not unique table/alias: '%s'"),

  ER_NO_SUCH_THREAD(1094, "HY000", "Unknown thread id: %lu"),

  ER_NET_READ_ERROR(1158, "08S01", "Got an error reading communication packets"),

  ER_DERIVED_MUST_HAVE_ALIAS(1248, "42000", "Every derived table must have its own alias"),

  ER_NO_SUCH_USER(1449, "HY000", "The user ('%s') does not exist"),

  ER_MALFORMED_PACKET(1835, "HY000", "Malformed communication packet.");

  private final int errorCode;

  private final String sqlState;

  private final String errorMessage;
}
