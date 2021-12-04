package com.gllue.transport.protocol.packet.handshake;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import com.gllue.common.util.RandomUtils;
import com.gllue.transport.BaseTransportTest;
import com.gllue.transport.constant.MySQLCapabilityFlag;
import com.gllue.transport.constant.MySQLServerInfo;
import com.gllue.transport.constant.MySQLStatusFlag;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class InitialHandshakePacketV10Test extends BaseTransportTest {

  private static final byte[] authPluginDataPart1 = RandomUtils.generateRandomBytes(8);
  private static final byte[] authPluginData = RandomUtils.generateRandomBytes(23);

  @Test
  public void testWriteWithPluginAuth() {
    var capabilityFlags = MySQLCapabilityFlag.unionCapabilityFlags(
        MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH
    );

    var packet = new InitialHandshakePacketV10(
        MySQLServerInfo.getServerVersion(),
        10,
        authPluginDataPart1,
        capabilityFlags,
        MySQLServerInfo.DEFAULT_CHARSET,
        MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT.getValue(),
        "auth plugin name"
    );

    var payload = createEmptyPayload();
    packet.write(payload);

    var newPacket = new InitialHandshakePacketV10(payload);
    assertThat(newPacket.getServerVersion(), equalTo(packet.getServerVersion()));
    assertThat(newPacket.getConnectionId(), equalTo(packet.getConnectionId()));
    assertThat(newPacket.getAuthPluginDataPart1(), equalTo(packet.getAuthPluginDataPart1()));
    assertThat(newPacket.getCapabilityFlags(), equalTo(packet.getCapabilityFlags()));
    assertThat(newPacket.getCharset(), equalTo(packet.getCharset()));
    assertThat(newPacket.getStatusFlags(), equalTo(packet.getStatusFlags()));
    assertThat(newPacket.getAuthPluginName(), equalTo(packet.getAuthPluginName()));
  }

  @Test
  public void testWriteWithoutPluginAuth() {
    var packet = new InitialHandshakePacketV10(
        MySQLServerInfo.getServerVersion(),
        10,
        authPluginDataPart1,
        0,
        MySQLServerInfo.DEFAULT_CHARSET,
        MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT.getValue(),
        null
    );

    var payload = createEmptyPayload();
    packet.write(payload);

    var newPacket = new InitialHandshakePacketV10(payload);
    assertThat(newPacket.getServerVersion(), equalTo(packet.getServerVersion()));
    assertThat(newPacket.getConnectionId(), equalTo(packet.getConnectionId()));
    assertThat(newPacket.getAuthPluginDataPart1(), equalTo(packet.getAuthPluginDataPart1()));
    assertThat(newPacket.getCapabilityFlags(), equalTo(packet.getCapabilityFlags()));
    assertThat(newPacket.getCharset(), equalTo(packet.getCharset()));
    assertThat(newPacket.getStatusFlags(), equalTo(packet.getStatusFlags()));
    assertThat(newPacket.getAuthPluginName(), equalTo(packet.getAuthPluginName()));
  }


  @Test
  public void testWriteWithPluginAuthAndSecureConn() {
    var capabilityFlags = MySQLCapabilityFlag.unionCapabilityFlags(
        MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH,
        MySQLCapabilityFlag.CLIENT_SECURE_CONNECTION
    );

    var packet = new InitialHandshakePacketV10(
        MySQLServerInfo.getServerVersion(),
        10,
        authPluginData,
        capabilityFlags,
        MySQLServerInfo.DEFAULT_CHARSET,
        MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT.getValue(),
        "auth plugin name"
    );

    var payload = createEmptyPayload();
    packet.write(payload);

    var newPacket = new InitialHandshakePacketV10(payload);
    assertThat(newPacket.getServerVersion(), equalTo(packet.getServerVersion()));
    assertThat(newPacket.getConnectionId(), equalTo(packet.getConnectionId()));
    assertThat(newPacket.getAuthPluginDataPart1(), equalTo(packet.getAuthPluginDataPart1()));
    assertThat(newPacket.getCapabilityFlags(), equalTo(packet.getCapabilityFlags()));
    assertThat(newPacket.getCharset(), equalTo(packet.getCharset()));
    assertThat(newPacket.getStatusFlags(), equalTo(packet.getStatusFlags()));
    assertThat(newPacket.getAuthPluginName(), equalTo(packet.getAuthPluginName()));
  }

}
