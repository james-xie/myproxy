package com.gllue.transport.protocol.packet.handshake;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import com.gllue.transport.BaseTransportTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AuthMoreDataPacketTest extends BaseTransportTest {
  @Test
  public void testWrite() {
    var packet = new AuthMoreDataPacket("plugin data");
    var payload = createEmptyPayload();
    packet.write(payload);
    var newPacket = new AuthMoreDataPacket(payload);

    assertThat(newPacket.getPluginData(), equalTo(packet.getPluginData()));
  }
}
