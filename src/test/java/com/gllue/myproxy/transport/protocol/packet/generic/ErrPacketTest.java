package com.gllue.myproxy.transport.protocol.packet.generic;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

import com.gllue.myproxy.transport.BaseTransportTest;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ErrPacketTest extends BaseTransportTest {
  @Test
  public void testWrite() {
    var packet = new ErrPacket(MySQLServerErrorCode.ER_BAD_DB_ERROR, "database name");
    var payload = createEmptyPayload();
    packet.write(payload);
    var newPacket = new ErrPacket(payload);

    assertThat(newPacket.getErrorCode(), equalTo(packet.getErrorCode()));
    assertThat(newPacket.getErrorMessage(), equalTo(packet.getErrorMessage()));
    assertThat(newPacket.getSqlState(), equalTo(packet.getSqlState()));
  }
}
