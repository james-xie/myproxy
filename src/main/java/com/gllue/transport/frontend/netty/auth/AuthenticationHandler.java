package com.gllue.transport.frontend.netty.auth;

import com.gllue.transport.core.connection.AuthenticationData;

public interface AuthenticationHandler {

  byte[] getAuthPluginData();

  boolean authenticate(AuthenticationData authData);
}
