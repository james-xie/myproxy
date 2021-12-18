package com.gllue.myproxy.transport.protocol.packet.handshake;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import com.gllue.myproxy.transport.BaseTransportTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AuthSwitchRequestPacketTest extends BaseTransportTest {
  @Test
  public void testWrite() {
    var packet = new AuthSwitchRequestPacket("plugin name", "auth plugin data");
    var payload = createEmptyPayload();
    packet.write(payload);
    var newPacket = new AuthSwitchRequestPacket(payload);

    assertThat(newPacket.getPluginName(), equalTo(packet.getPluginName()));
    assertThat(newPacket.getAuthPluginData(), equalTo(packet.getAuthPluginData()));
  }
}
