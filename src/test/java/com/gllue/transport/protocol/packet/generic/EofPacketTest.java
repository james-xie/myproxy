package com.gllue.transport.protocol.packet.generic;

import com.gllue.transport.constant.MySQLStatusFlag;
import com.gllue.transport.BaseTransportTest;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EofPacketTest extends BaseTransportTest {
  @Test
  public void testWrite() {
    var packet = new EofPacket(1, MySQLStatusFlag.SERVER_STATUS_DB_DROPPED.getValue());
    var payload = createEmptyPayload();
    packet.write(payload);
    var newPacket = new EofPacket(payload);

    assertThat(newPacket.getWarnings(), equalTo(packet.getWarnings()));
    assertThat(newPacket.getStatusFlags(), equalTo(packet.getStatusFlags()));
  }
}
