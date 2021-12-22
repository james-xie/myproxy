package com.gllue.myproxy.transport.protocol.packet.handshake;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.transport.BaseTransportTest;
import com.gllue.myproxy.transport.constant.MySQLCapabilityFlag;
import com.gllue.myproxy.transport.constant.MySQLServerInfo;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HandshakeResponsePacket41Test extends BaseTransportTest {
  static final int SERVER_CAPABILITIES = MySQLCapabilityFlag.handshakeServerCapabilityFlags();

  @Test
  public void testConnectWithDatabase() {
    var capabilityFlags = MySQLCapabilityFlag.unionCapabilityFlags(
        MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH,
        MySQLCapabilityFlag.CLIENT_CONNECT_WITH_DB
    );

    var packet = new HandshakeResponsePacket41(
        capabilityFlags,
        MySQLPacket.MAX_PACKET_SIZE,
        MySQLServerInfo.DEFAULT_CHARSET,
        "test user",
        RandomUtils.generateRandomBytes(20),
        "test database",
        "auth plugin name"
    );

    var payload = createEmptyPayload();
    packet.write(payload);

    var newPacket = new HandshakeResponsePacket41(payload);
    assertThat(newPacket.getClientCapabilityFlags(), equalTo(packet.getClientCapabilityFlags()));
    assertThat(newPacket.getMaxPacketSize(), equalTo(packet.getMaxPacketSize()));
    assertThat(newPacket.getCharset(), equalTo(packet.getCharset()));
    assertThat(newPacket.getUsername(), equalTo(packet.getUsername()));
    assertThat(newPacket.getAuthResponse(), equalTo(packet.getAuthResponse()));
    assertThat(newPacket.getDatabase(), equalTo(packet.getDatabase()));
    assertThat(newPacket.getAuthPluginName(), equalTo(packet.getAuthPluginName()));
  }

  @Test
  public void testConnectWithAttributes() {
    var capabilityFlags = MySQLCapabilityFlag.unionCapabilityFlags(
        MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH,
        MySQLCapabilityFlag.CLIENT_CONNECT_WITH_DB,
        MySQLCapabilityFlag.CLIENT_CONNECT_ATTRS
    );

    var packet = new HandshakeResponsePacket41(
        capabilityFlags,
        MySQLPacket.MAX_PACKET_SIZE,
        MySQLServerInfo.DEFAULT_CHARSET,
        "test user",
        RandomUtils.generateRandomBytes(20),
        "test database",
        "auth plugin name"
    );
    packet.addConnectAttribute("key1", "value1");
    packet.addConnectAttribute("key2", "value2");
    packet.addConnectAttribute("key3", "value3");

    var payload = createEmptyPayload();
    packet.write(payload);

    var newPacket = new HandshakeResponsePacket41(payload);
    assertThat(newPacket.getClientCapabilityFlags(), equalTo(packet.getClientCapabilityFlags()));
    assertThat(newPacket.getConnectAttrs(), equalTo(packet.getConnectAttrs()));
  }

  @Test
  public void testConnectWithSecureConnectFlag() {
    var capabilityFlags = MySQLCapabilityFlag.unionCapabilityFlags(
        MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH,
        MySQLCapabilityFlag.CLIENT_CONNECT_WITH_DB,
        MySQLCapabilityFlag.CLIENT_CONNECT_ATTRS,
        MySQLCapabilityFlag.CLIENT_SECURE_CONNECTION
    );

    var packet = new HandshakeResponsePacket41(
        capabilityFlags,
        MySQLPacket.MAX_PACKET_SIZE,
        MySQLServerInfo.DEFAULT_CHARSET,
        "test user",
        RandomUtils.generateRandomBytes(20),
        "test database",
        "auth plugin name"
    );
    packet.addConnectAttribute("key1", "value1");
    packet.addConnectAttribute("key2", "value2");
    packet.addConnectAttribute("key3", "value3");

    var payload = createEmptyPayload();
    packet.write(payload);

    var newPacket = new HandshakeResponsePacket41(payload);
    assertThat(newPacket.getClientCapabilityFlags(), equalTo(packet.getClientCapabilityFlags()));
    assertThat(newPacket.getAuthResponse(), equalTo(packet.getAuthResponse()));
  }
}
