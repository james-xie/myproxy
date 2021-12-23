package com.gllue.myproxy.transport.constant;


import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MySQLServerInfo {

  /**
   * Protocol version is always 0x0A.
   */
  public static final int PROTOCOL_VERSION = 0x0A;

  public static final int DEFAULT_CHARSET = MySQLCharsets.UTF8MB4_CHARSET_ID;

}