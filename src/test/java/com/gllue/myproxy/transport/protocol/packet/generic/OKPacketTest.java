package com.gllue.myproxy.transport.protocol.packet.generic;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import com.gllue.myproxy.transport.BaseTransportTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OKPacketTest extends BaseTransportTest {
  @Test
  public void testWrite() {
    var packet = new OKPacket(999, 1000);
    var payload = createEmptyPayload();
    packet.write(payload);
    var newPacket = new OKPacket(payload);

    assertThat(newPacket.getAffectedRows(), equalTo(packet.getAffectedRows()));
    assertThat(newPacket.getLastInsertId(), equalTo(packet.getLastInsertId()));
  }
}
