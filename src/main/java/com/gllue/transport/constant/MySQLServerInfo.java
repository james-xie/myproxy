package com.gllue.transport.constant;


import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MySQLServerInfo {

  /**
   * Protocol version is always 0x0A.
   */
  public static final int PROTOCOL_VERSION = 0x0A;

  public static final int DEFAULT_CHARSET = MySQLCharsets.UTF8MB4_CHARSET_ID;

  private static final String PROXY_VERSION = "8.0.0";

  private static final String SERVER_VERSION_PATTERN = "%s [MySQL Proxy]";

  /**
   * Get current server version.
   *
   * @return server version
   */
  public static String getServerVersion() {
    return String.format(SERVER_VERSION_PATTERN, PROXY_VERSION);
  }
}