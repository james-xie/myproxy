package com.gllue.myproxy.transport.protocol.packet.command;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import com.gllue.myproxy.transport.BaseTransportTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

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
