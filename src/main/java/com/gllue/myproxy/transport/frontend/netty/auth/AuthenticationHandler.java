package com.gllue.myproxy.transport.frontend.netty.auth;

import com.gllue.myproxy.transport.core.connection.AuthenticationData;

public interface AuthenticationHandler {

  byte[] getAuthPluginData();

  boolean authenticate(AuthenticationData authData);
}
