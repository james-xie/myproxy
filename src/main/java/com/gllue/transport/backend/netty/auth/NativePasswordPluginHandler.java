package com.gllue.transport.backend.netty.auth;

import com.gllue.common.util.SecurityUtils;
import com.gllue.transport.core.connection.AuthenticationData;
import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.packet.generic.GenericPacketWrapper;
import com.gllue.transport.protocol.packet.handshake.AuthSwitchRequestPacket;
import com.gllue.transport.protocol.packet.handshake.AuthSwitchResponsePacket;
import com.gllue.transport.protocol.payload.MySQLPayload;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NativePasswordPluginHandler implements AuthenticationPluginHandler {
  private static final int SCRAMBLE_LENGTH = 20;

  @RequiredArgsConstructor
  public enum State implements AuthenticationState {
    INITIAL(INITIAL_STATE_NAME),
    CHECK_RESPONSE("check_response"),
    SUCCESS(SUCCESS_STATE_NAME),
    FAILED(FAILED_STATE_NAME);

    @Getter private final String name;

    @Override
    public AuthenticationState initialState() {
      return INITIAL;
    }

    @Override
    public AuthenticationState successState() {
      return SUCCESS;
    }

    @Override
    public AuthenticationState failedState() {
      return FAILED;
    }
  }

  @Override
  public byte[] scramblePassword(String password, byte[] salt) throws Exception {
    var stage1 = SecurityUtils.sha1Digest(password.getBytes());
    var stage2 = SecurityUtils.sha1Digest(stage1);
    var saltPrefix = new byte[SCRAMBLE_LENGTH];
    System.arraycopy(salt, 0, saltPrefix, 0, SCRAMBLE_LENGTH);
    var stage3 = SecurityUtils.sha1Digest(saltPrefix, stage2);
    return scramble(stage3, stage1);
  }

  @Override
  public AuthenticationState authenticate(
      ChannelHandlerContext ctx,
      MySQLPayload payload,
      MySQLPacket packet,
      AuthenticationState currentState,
      AuthenticationData authData)
      throws Exception {
    AuthenticationState nextState;
    if (currentState == State.INITIAL) {
      assert packet instanceof AuthSwitchRequestPacket;
      var authSwitchRequest = (AuthSwitchRequestPacket) packet;
      authData.setAuthResponse(authSwitchRequest.getAuthPluginData().getBytes());
      nextState = handleAuthSwitchRequest(ctx, authData);
    } else if (currentState == State.CHECK_RESPONSE) {
      assert payload != null;
      nextState = checkResponse(payload);
    } else {
      throw new IllegalStateException(currentState.getName());
    }
    return nextState;
  }

  private AuthenticationState handleAuthSwitchRequest(
      final ChannelHandlerContext ctx, final AuthenticationData authData) {
    byte[] authPluginData;
    try {
      authPluginData = scramblePassword(authData.getPassword(), authData.getAuthResponse());
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return State.FAILED;
    }

    ctx.writeAndFlush(new AuthSwitchResponsePacket(authPluginData));
    return State.CHECK_RESPONSE;
  }

  private AuthenticationState checkResponse(final MySQLPayload payload) {
    var wrapper = GenericPacketWrapper.newInstance(payload);
    return wrapper.isOkPacket() ? State.SUCCESS : State.FAILED;
  }

  @Override
  public AuthenticationState getInitialState() {
    return State.INITIAL;
  }

  private byte[] scramble(final byte[] result, final byte[] factor) {
    for (int i = 0; i < result.length; i++) {
      result[i] ^= factor[i];
    }
    return result;
  }
}
