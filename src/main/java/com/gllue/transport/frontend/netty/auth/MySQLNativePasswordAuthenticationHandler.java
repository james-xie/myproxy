package com.gllue.transport.frontend.netty.auth;

import com.gllue.common.util.RandomUtils;
import com.gllue.transport.core.connection.AuthenticationData;
import com.gllue.transport.frontend.netty.auth.AuthenticationHandler;
import lombok.Getter;

public class MySQLNativePasswordAuthenticationHandler implements AuthenticationHandler {

  private static final int AUTH_PLUGIN_DATA_PART_1_LENGTH = 8;
  private static final int AUTH_PLUGIN_DATA_PART_2_LENGTH = 12;

  @Getter private final byte[] authPluginData;

  public MySQLNativePasswordAuthenticationHandler() {
    authPluginData =
        RandomUtils.generateRandomBytes(
            AUTH_PLUGIN_DATA_PART_1_LENGTH + AUTH_PLUGIN_DATA_PART_2_LENGTH);
  }

  @Override
  public boolean authenticate(AuthenticationData authData) {
    // todo: implement authenticate
    return true;
  }
}
