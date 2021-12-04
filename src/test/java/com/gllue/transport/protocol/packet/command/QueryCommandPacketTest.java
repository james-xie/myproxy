package com.gllue.transport.protocol.packet.command;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import com.gllue.transport.BaseTransportTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryCommandPacketTest extends BaseTransportTest {
  @Test
  public void testReadWrite() {
    var packet = new QueryCommandPacket("select 1");
    var payload = createEmptyPayload();
    packet.write(payload);
    var newPacket = new QueryCommandPacket(payload);

    assertThat(newPacket.getQuery(), equalTo(packet.getQuery()));
  }
}
