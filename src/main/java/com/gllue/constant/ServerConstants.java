package com.gllue.constant;

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

}
