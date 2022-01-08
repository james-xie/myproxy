package com.gllue.myproxy.transport.backend.netty.auth;

import com.gllue.myproxy.common.util.SecurityUtils;
import com.gllue.myproxy.transport.core.connection.AuthenticationData;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.packet.PacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.generic.GenericPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.generic.RawPacket;
import com.gllue.myproxy.transport.protocol.packet.handshake.AuthMoreDataPacket;
import com.gllue.myproxy.transport.protocol.packet.handshake.AuthPacketWrapper;
import com.gllue.myproxy.transport.protocol.packet.handshake.AuthSwitchRequestPacket;
import com.gllue.myproxy.transport.protocol.packet.handshake.AuthSwitchResponsePacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.google.common.primitives.Bytes;
import io.netty.channel.ChannelHandlerContext;
import java.security.MessageDigest;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Caching-sha2 authentication handler.
 *
 * <pre>
 * @see <a href="https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html#sect_caching_sha2_info"></a>
 * </pre>
 */
@Slf4j
public class CachingSha2PluginHandler implements AuthenticationPluginHandler {
  private static final int FAST_AUTH_SUCCEED_FLAG = 3;
  private static final int NEED_FULL_AUTH_FLAG = 4;
  private static final int CACHING_SHA2_DIGEST_LENGTH = 32;
  private static final RawPacket REQUEST_PUBLIC_KEY_PACKET = new RawPacket(new byte[] {0x02});

  @RequiredArgsConstructor
  public enum State implements AuthenticationState {
    INITIAL(INITIAL_STATE_NAME),
    CHECK_RESPONSE("check_response"),
    CHECK_FAST_AUTH_RESULT("check_fast_auth_result"),
    FULL_AUTH("full_auth"),
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

  private AuthenticationState nextStateAfterCheckResponse;

  public static void xorString(byte[] from, byte[] to, byte[] scramble, int length) {
    int pos = 0;

    for (int scrambleLength = scramble.length; pos < length; ++pos) {
      to[pos] = (byte) (from[pos] ^ scramble[pos % scrambleLength]);
    }
  }

  @Override
  public byte[] scramblePassword(String password, byte[] salt) throws Exception {
    if (password == null || password.isBlank()) {
      return new byte[0];
    }

    var passwordBytes = password.getBytes();

    MessageDigest md = SecurityUtils.sha256Instance();
    byte[] dig1 = new byte[CACHING_SHA2_DIGEST_LENGTH];
    byte[] dig2 = new byte[CACHING_SHA2_DIGEST_LENGTH];
    byte[] scramble1 = new byte[CACHING_SHA2_DIGEST_LENGTH];
    md.update(passwordBytes, 0, passwordBytes.length);
    md.digest(dig1, 0, CACHING_SHA2_DIGEST_LENGTH);
    md.reset();
    md.update(dig1, 0, dig1.length);
    md.digest(dig2, 0, CACHING_SHA2_DIGEST_LENGTH);
    md.reset();
    md.update(dig2, 0, dig1.length);
    md.update(salt, 0, salt.length);
    md.digest(scramble1, 0, CACHING_SHA2_DIGEST_LENGTH);
    byte[] mysqlScrambleBuff = new byte[CACHING_SHA2_DIGEST_LENGTH];
    xorString(dig1, mysqlScrambleBuff, scramble1, CACHING_SHA2_DIGEST_LENGTH);
    return mysqlScrambleBuff;
  }

  @Override
  public AuthenticationState authenticate(
      ChannelHandlerContext ctx,
      MySQLPayload payload,
      MySQLPacket packet,
      AuthenticationState currentState,
      AuthenticationData authData)
      throws Exception {
    AuthenticationState nextState = State.FAILED;

    boolean autoTransferState = true;
    while (autoTransferState) {
      autoTransferState = false;

      if (currentState == State.INITIAL) {
        assert packet != null;

        var password = authData.getPassword();
        if (password == null || password.isBlank()) {
          nextState = handleEmptyPassword(ctx);
        } else if (packet instanceof AuthSwitchRequestPacket) {
          // try for fast path
          nextState = handleAuthSwitchRequest(ctx, authData, (AuthSwitchRequestPacket) packet);
        } else if (packet instanceof AuthMoreDataPacket) {
          nextState = checkFastAuthResult(ctx, (AuthMoreDataPacket) packet);
        } else {
          throw new IllegalStateException(
              String.format("Unknown packet for fast auth. [%s]", packet));
        }
      } else if (currentState == State.CHECK_RESPONSE) {
        nextState = checkResponse(payload);
        if (nextState != State.SUCCESS && nextState != State.FAILED) {
          autoTransferState = true;
        }
      } else if (currentState == State.CHECK_FAST_AUTH_RESULT) {
        var wrapper = AuthPacketWrapper.newInstance(payload);
        if (!wrapper.isAuthMoreDataPacket()) {
          throw new IllegalStateException(
              String.format(
                  "Got an unexpected packet for fast auth. [%s]", formatPacketWrapper(wrapper)));
        }
        nextState = checkFastAuthResult(ctx, (AuthMoreDataPacket) wrapper.getPacket());
      } else if (currentState == State.FULL_AUTH) {
        nextState = fullAuth(ctx, payload, authData);
      } else {
        throw new IllegalStateException(currentState.getName());
      }

      currentState = nextState;
    }
    return nextState;
  }

  private AuthenticationState handleEmptyPassword(final ChannelHandlerContext ctx) {
    ctx.writeAndFlush(new AuthSwitchResponsePacket(new byte[0]));
    nextStateAfterCheckResponse = null;
    return State.CHECK_RESPONSE;
  }

  private AuthenticationState handleAuthSwitchRequest(
      final ChannelHandlerContext ctx,
      final AuthenticationData authData,
      final AuthSwitchRequestPacket packet)
      throws Exception {
    authData.setAuthResponse(packet.getAuthPluginData().getBytes());
    var authResponse = scramblePassword(authData.getPassword(), authData.getAuthResponse());
    ctx.writeAndFlush(new AuthSwitchResponsePacket(authResponse));
    return State.CHECK_FAST_AUTH_RESULT;
  }

  private AuthenticationState checkResponse(final MySQLPayload payload) {
    AuthenticationState nextState;
    var wrapper = GenericPacketWrapper.newInstance(payload);
    if (wrapper.isOkPacket()) {
      nextState = nextStateAfterCheckResponse == null ? State.SUCCESS : nextStateAfterCheckResponse;
      nextStateAfterCheckResponse = null;
    } else {
      log.error(
          String.format(
              "Got an unexpected packet when invoking checkResponse. [%s]",
              formatPacketWrapper(wrapper)));
      nextState = State.FAILED;
    }
    return nextState;
  }

  private AuthenticationState checkFastAuthResult(
      final ChannelHandlerContext ctx, final AuthMoreDataPacket packet) {
    int flag = 0;
    if (!packet.getPluginData().isEmpty()) {
      flag = packet.getPluginData().getBytes()[0];
    }

    switch (flag) {
      case FAST_AUTH_SUCCEED_FLAG:
        {
          if (log.isDebugEnabled()) {
            log.debug("Succeeded by fast path.");
          }
          nextStateAfterCheckResponse = null;
          return State.CHECK_RESPONSE;
        }
      case NEED_FULL_AUTH_FLAG:
        {
          // trying full auth
          if (log.isDebugEnabled()) {
            log.debug("Trying full auth.");
          }
          ctx.writeAndFlush(REQUEST_PUBLIC_KEY_PACKET);
          return State.FULL_AUTH;
        }
      default:
        throw new IllegalStateException("Illegal auth more data packet for fast auth.");
    }
  }

  private AuthenticationState fullAuth(
      final ChannelHandlerContext ctx,
      final MySQLPayload payload,
      final AuthenticationData authData)
      throws Exception {
    var wrapper = AuthPacketWrapper.newInstance(payload);
    if (!wrapper.isAuthMoreDataPacket()) {
      throw new IllegalStateException(
          String.format(
              "Got an unexpected packet when invoking fullAuth. [%s]",
              formatPacketWrapper(wrapper)));
    }
    var packet = (AuthMoreDataPacket) wrapper.getPacket();
    var serverPublicKey = packet.getPluginData();
    ctx.writeAndFlush(new RawPacket(sha2RsaEncrypt(authData, serverPublicKey)));
    nextStateAfterCheckResponse = null;
    return State.CHECK_RESPONSE;
  }

  private byte[] sha2RsaEncrypt(final AuthenticationData authData, final String serverPublicKey)
      throws Exception {
    // todo: Encrypt with "RSA/ECB/OAEPWithSHA-1AndMGF1Padding" transformation if the server version
    //   >= 8.0.5, otherwise using "RSA/ECB/PKCS1Padding"
    byte[] input = Bytes.concat(authData.getPassword().getBytes(), new byte[] {0});
    byte[] scrambleBytes = new byte[input.length];
    xorString(input, scrambleBytes, authData.getAuthResponse(), input.length);
    var publicKey = SecurityUtils.decodeRSAPublicKey(serverPublicKey);
    return SecurityUtils.encryptWithRSAPublicKey(
        scrambleBytes, publicKey, SecurityUtils.RSA_SHA1_MGF1_PADDING_TRANSFORMATION);
  }

  @Override
  public AuthenticationState getInitialState() {
    return State.INITIAL;
  }

  private String formatPacketWrapper(final PacketWrapper wrapper) {
    return wrapper.getPacketDescription();
  }
}
