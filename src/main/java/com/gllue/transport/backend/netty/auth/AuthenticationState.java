package com.gllue.transport.backend.netty.auth;

public interface AuthenticationState {
  String INITIAL_STATE_NAME = "initial";
  String SUCCESS_STATE_NAME = "success";
  String FAILED_STATE_NAME = "failed";

  String getName();

  AuthenticationState initialState();

  AuthenticationState successState();

  AuthenticationState failedState();
}
