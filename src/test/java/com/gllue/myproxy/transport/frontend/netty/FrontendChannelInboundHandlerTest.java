package com.gllue.myproxy.transport.frontend.netty;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;

import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.transport.BaseTransportTest;
import com.gllue.myproxy.transport.constant.MySQLAuthenticationMethod;
import com.gllue.myproxy.transport.constant.MySQLCapabilityFlag;
import com.gllue.myproxy.transport.constant.MySQLServerInfo;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.frontend.command.CommandExecutionEngine;
import com.gllue.myproxy.transport.frontend.netty.auth.AuthenticationHandler;
import com.gllue.myproxy.transport.protocol.packet.handshake.AuthSwitchRequestPacket;
import com.gllue.myproxy.transport.protocol.packet.handshake.AuthSwitchResponsePacket;
import com.gllue.myproxy.transport.protocol.packet.handshake.HandshakeResponsePacket41;
import com.gllue.myproxy.transport.frontend.connection.FrontendConnectionManager;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FrontendChannelInboundHandlerTest extends BaseTransportTest {

  static final int CLIENT_CAPABILITIES =
      MySQLCapabilityFlag.unionCapabilityFlags(
          MySQLCapabilityFlag.CLIENT_LONG_PASSWORD,
          MySQLCapabilityFlag.CLIENT_LONG_FLAG,
          MySQLCapabilityFlag.CLIENT_PROTOCOL_41,
          MySQLCapabilityFlag.CLIENT_TRANSACTIONS,
          MySQLCapabilityFlag.CLIENT_SECURE_CONNECTION,
          MySQLCapabilityFlag.CLIENT_MULTI_RESULTS,
          MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH,
          MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA,
          MySQLCapabilityFlag.CLIENT_CONNECT_ATTRS,
          MySQLCapabilityFlag.CLIENT_CONNECT_WITH_DB);

  static final byte[] AUTH_PLUGIN_DATA;

  static String FORMATTED_USER_NAME = "source1#user";

  static String DATABASE_NAME = "db1";

  static {
    AUTH_PLUGIN_DATA = RandomUtils.generateRandomBytes(20);
  }

  @Mock
  AuthenticationHandler authHandler;
  @Mock
  CommandExecutionEngine commandExecutionEngine;
  @Mock FrontendConnectionManager frontendConnectionManager;

  @Test
  public void testAuthenticateSuccess() {
    Mockito.when(authHandler.authenticate(any())).thenReturn(true);
    Mockito.when(authHandler.getAuthPluginData()).thenReturn(AUTH_PLUGIN_DATA);

    EmbeddedChannel ch = prepareChannel();

    var handshakeResponse =
        new HandshakeResponsePacket41(
            CLIENT_CAPABILITIES,
            MySQLPacket.MAX_PACKET_SIZE,
            MySQLServerInfo.DEFAULT_CHARSET,
            FORMATTED_USER_NAME,
            RandomUtils.generateRandomBytes(20),
            DATABASE_NAME,
            MySQLAuthenticationMethod.NATIVE_PASSWORD.getMethodName());

    var payload = packetToPayload(handshakeResponse);
    ch.writeInbound(payload);
    assertOkPacket(ch.readOutbound());
    assertPayloadClosed(payload);

    ch.finish();
  }

  @Test
  public void testAuthenticateFailed() {
    Mockito.when(authHandler.authenticate(any())).thenReturn(false);
    Mockito.when(authHandler.getAuthPluginData()).thenReturn(AUTH_PLUGIN_DATA);

    EmbeddedChannel ch = prepareChannel();

    var handshakeResponse =
        new HandshakeResponsePacket41(
            CLIENT_CAPABILITIES,
            MySQLPacket.MAX_PACKET_SIZE,
            MySQLServerInfo.DEFAULT_CHARSET,
            FORMATTED_USER_NAME,
            RandomUtils.generateRandomBytes(20),
            DATABASE_NAME,
            MySQLAuthenticationMethod.NATIVE_PASSWORD.getMethodName());

    var payload = packetToPayload(handshakeResponse);
    ch.writeInbound(payload);
    assertErrorPacket(ch.readOutbound(), MySQLServerErrorCode.ER_ACCESS_DENIED_ERROR);
    assertPayloadClosed(payload);

    ch.finish();
  }

  @Test
  public void testInvalidDatabaseName() {
    Mockito.when(authHandler.getAuthPluginData()).thenReturn(AUTH_PLUGIN_DATA);

    for (var dbname : List.of("dbname1", "#dbname1", "dbname1#")) {
      EmbeddedChannel ch = prepareChannel();
      var handshakeResponse =
          new HandshakeResponsePacket41(
              CLIENT_CAPABILITIES,
              MySQLPacket.MAX_PACKET_SIZE,
              MySQLServerInfo.DEFAULT_CHARSET,
              "user",
              RandomUtils.generateRandomBytes(20),
              dbname,
              MySQLAuthenticationMethod.NATIVE_PASSWORD.getMethodName());

      var payload = packetToPayload(handshakeResponse);
      ch.writeInbound(payload);
      assertErrorPacket(ch.readOutbound(), MySQLServerErrorCode.ER_NO_SUCH_USER);
      assertPayloadClosed(payload);
      ch.finish();
    }
  }

  @Test
  public void testAuthSwitch() {
    Mockito.when(authHandler.authenticate(any())).thenReturn(true);
    Mockito.when(authHandler.getAuthPluginData()).thenReturn(AUTH_PLUGIN_DATA);

    EmbeddedChannel ch = prepareChannel();

    var handshakeResponse =
        new HandshakeResponsePacket41(
            CLIENT_CAPABILITIES,
            MySQLPacket.MAX_PACKET_SIZE,
            MySQLServerInfo.DEFAULT_CHARSET,
            FORMATTED_USER_NAME,
            RandomUtils.generateRandomBytes(20),
            DATABASE_NAME,
            MySQLAuthenticationMethod.SHA256.getMethodName());

    ch.writeInbound(packetToPayload(handshakeResponse));
    var message = ch.readOutbound();
    assertThat(message, instanceOf(AuthSwitchRequestPacket.class));
    var authSwitchRequestPacket = (AuthSwitchRequestPacket) message;
    assertThat(
        authSwitchRequestPacket.getPluginName(),
        equalTo(MySQLAuthenticationMethod.NATIVE_PASSWORD.getMethodName()));
    assertThat(authSwitchRequestPacket.getAuthPluginData(), equalTo(new String(AUTH_PLUGIN_DATA)));

    var authSwitchResponsePacket =
        new AuthSwitchResponsePacket(RandomUtils.generateRandomBytes(20));

    var payload = packetToPayload(authSwitchResponsePacket);
    ch.writeInbound(payload);
    assertOkPacket(ch.readOutbound());
    assertPayloadClosed(payload);

    ch.finish();
  }

  private EmbeddedChannel prepareChannel() {
    //    Mockito.doNothing().when(commandExecutionEngine).execute(any(), any());

    var ch =
        new EmbeddedChannel(
            new FrontendChannelInboundHandler(
                authHandler, commandExecutionEngine, frontendConnectionManager));
    assertNotNull(ch.readOutbound());
    return ch;
  }
}
