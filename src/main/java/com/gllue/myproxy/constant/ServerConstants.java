package com.gllue.myproxy.constant;

import com.gllue.myproxy.transport.constant.MySQLServerInfo;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ServerConstants {
  /**
   * Path separator.
   */
  public static final String PATH_SEPARATOR = "/";

  /**
   * Persistence path for databases meta data.
   */
  public static final String DATABASES_ROOT_PATH = "databases";

  /**
   * A symbol which is used to quote mysql identifier.
   */
  public static final String MYSQL_QUOTE_SYMBOL = "`";

  /**
   * A symbol which is used to represent all columns.
   */
  public static final String ALL_COLUMN_EXPR = "*";

  public static final String PROXY_VERSION = "5.7.0";

  public static final String SERVER_VERSION_PATTERN = "%s [MySQL Proxy]";

  /**
   * Get current server version.
   *
   * @return server version
   */
  public static String getServerVersion() {
    return String.format(SERVER_VERSION_PATTERN, PROXY_VERSION);
  }
}
