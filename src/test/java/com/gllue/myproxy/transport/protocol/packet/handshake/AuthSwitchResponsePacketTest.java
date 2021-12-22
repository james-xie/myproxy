package com.gllue.myproxy.transport.protocol.packet.handshake;


import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import com.gllue.myproxy.transport.BaseTransportTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AuthSwitchResponsePacketTest extends BaseTransportTest {
  @Test
  public void testWrite() {
    var packet = new AuthSwitchResponsePacket("auth plugin data".getBytes());
    var payload = createEmptyPayload();
    packet.write(payload);
    var newPacket = new AuthSwitchResponsePacket(payload);

    assertThat(newPacket.getAuthPluginData(), equalTo(packet.getAuthPluginData()));
  }
}
