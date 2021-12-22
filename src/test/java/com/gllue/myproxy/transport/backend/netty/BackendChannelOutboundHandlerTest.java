package com.gllue.myproxy.transport.backend.netty;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.transport.BaseTransportTest;
import com.gllue.myproxy.transport.backend.connection.BackendConnectionListener;
import com.gllue.myproxy.transport.backend.connection.ConnectionArguments;
import com.gllue.myproxy.transport.constant.MySQLAuthenticationMethod;
import com.gllue.myproxy.transport.constant.MySQLCapabilityFlag;
import com.gllue.myproxy.transport.constant.MySQLServerInfo;
import com.gllue.myproxy.transport.constant.MySQLStatusFlag;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.OKPacket;
import com.gllue.myproxy.transport.protocol.packet.handshake.HandshakeResponsePacket41;
import com.gllue.myproxy.transport.protocol.packet.handshake.InitialHandshakePacketV10;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BackendChannelOutboundHandlerTest extends BaseTransportTest {

  static final int CONNECTION_ID = 1;

  static final byte[] AUTH_PLUGIN_DATA;

  static {
    AUTH_PLUGIN_DATA = RandomUtils.generateRandomBytes(20);
  }

  private static final int CLIENT_CAPABILITIES =
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

  @Mock
  ConnectionArguments connectionArguments;
  @Mock
  BackendConnectionListener backendConnectionListener;

  @Test
  public void testConnectedSuccess() {
    Mockito.when(connectionArguments.getUsername()).thenReturn("root");
    Mockito.when(connectionArguments.getPassword()).thenReturn("passwd");
    Mockito.when(connectionArguments.getDatabase()).thenReturn("db1");
    Mockito.when(backendConnectionListener.onConnected(any())).thenReturn(true);

    EmbeddedChannel ch = prepareChannel();

    // initial handshake phase.
    var initialHandshakePacket =
        new InitialHandshakePacketV10(
            MySQLServerInfo.getServerVersion(),
            CONNECTION_ID,
            AUTH_PLUGIN_DATA,
            CLIENT_CAPABILITIES,
            MySQLServerInfo.DEFAULT_CHARSET,
            MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT.getValue(),
            MySQLAuthenticationMethod.NATIVE_PASSWORD.getMethodName());

    var payload = packetToPayload(initialHandshakePacket);
    ch.writeInbound(payload);
    var handshakeResponsePacket = (HandshakeResponsePacket41) ch.readOutbound();
    assertNotNull(handshakeResponsePacket);
    assertThat(handshakeResponsePacket, instanceOf(HandshakeResponsePacket41.class));
    assertPayloadClosed(payload);

    // check response phase.
    payload = packetToPayload(new OKPacket());
    ch.writeInbound(payload);
    Mockito.verify(backendConnectionListener, times(1)).onConnected(any());
    Mockito.verify(backendConnectionListener, times(0)).onConnectFailed(any());
    Mockito.verify(backendConnectionListener, times(0)).onClosed(any());

    ch.close();
  }

  @Test
  public void testConnectedFailed() {
    Mockito.when(connectionArguments.getUsername()).thenReturn("root");
    Mockito.when(connectionArguments.getPassword()).thenReturn("passwd");
    Mockito.when(connectionArguments.getDatabase()).thenReturn("db1");
//    Mockito.doNothing().when(backendConnectionListener).onConnectFailed(any());
//    Mockito.doNothing().when(backendConnectionListener).onClosed(any());

    EmbeddedChannel ch = prepareChannel();

    // initial handshake phase.
    var initialHandshakePacket =
        new InitialHandshakePacketV10(
            MySQLServerInfo.getServerVersion(),
            CONNECTION_ID,
            AUTH_PLUGIN_DATA,
            CLIENT_CAPABILITIES,
            MySQLServerInfo.DEFAULT_CHARSET,
            MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT.getValue(),
            MySQLAuthenticationMethod.NATIVE_PASSWORD.getMethodName());

    var payload = packetToPayload(initialHandshakePacket);
    ch.writeInbound(payload);
    var handshakeResponsePacket = (HandshakeResponsePacket41) ch.readOutbound();
    assertNotNull(handshakeResponsePacket);
    assertThat(handshakeResponsePacket, instanceOf(HandshakeResponsePacket41.class));
    assertPayloadClosed(payload);

    // check response phase.
    payload = packetToPayload(new ErrPacket(MySQLServerErrorCode.ER_NO_DB_ERROR));
    ch.writeInbound(payload);
    Mockito.verify(backendConnectionListener, times(0)).onConnected(any());
    Mockito.verify(backendConnectionListener, times(1)).onConnectFailed(any());
    Mockito.verify(backendConnectionListener, times(1)).onClosed(any());
    ch.close();
  }

  @Test
  public void testConnectionClose() {
    Mockito.when(connectionArguments.getUsername()).thenReturn("root");
    Mockito.when(connectionArguments.getPassword()).thenReturn("passwd");
    Mockito.when(connectionArguments.getDatabase()).thenReturn("db1");

    EmbeddedChannel ch = prepareChannel();

    // initial handshake phase.
    var initialHandshakePacket =
        new InitialHandshakePacketV10(
            MySQLServerInfo.getServerVersion(),
            CONNECTION_ID,
            AUTH_PLUGIN_DATA,
            CLIENT_CAPABILITIES,
            MySQLServerInfo.DEFAULT_CHARSET,
            MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT.getValue(),
            MySQLAuthenticationMethod.NATIVE_PASSWORD.getMethodName());

    var payload = packetToPayload(initialHandshakePacket);
    ch.writeInbound(payload);

    ch.close();

    Mockito.verify(backendConnectionListener, times(0)).onConnected(any());
    Mockito.verify(backendConnectionListener, times(0)).onConnectFailed(any());
    Mockito.verify(backendConnectionListener, times(1)).onClosed(any());
  }

  @Test
  public void testCachingSha1AuthPlugin() {
    // todo
  }

  @Test
  public void testAuthSwitch() {
    // todo
  }

  private EmbeddedChannel prepareChannel() {
    var handler = new BackendChannelOutboundHandler(connectionArguments, backendConnectionListener);
    return new EmbeddedChannel(handler);
  }
}
