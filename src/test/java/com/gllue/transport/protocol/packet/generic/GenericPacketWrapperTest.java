package com.gllue.transport.protocol.packet.generic;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.gllue.transport.BaseTransportTest;
import com.gllue.transport.exception.MySQLServerErrorCode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GenericPacketWrapperTest extends BaseTransportTest {
  @Test
  public void testWrappedOkPacket() {
    var packet = new OKPacket();
    var payload = createEmptyPayload();
    packet.write(payload);
    var wrapper = GenericPacketWrapper.newInstance(payload);
    assertTrue(wrapper.isOkPacket());
  }

  @Test
  public void testWrappedErrPacket() {
    var packet = new ErrPacket(MySQLServerErrorCode.ER_NO_DB_ERROR);
    var payload = createEmptyPayload();
    packet.write(payload);
    var wrapper = GenericPacketWrapper.newInstance(payload);
    assertTrue(wrapper.isErrPacket());
  }

  @Test
  public void testWrappedEofPacket() {
    var packet = new EofPacket();
    var payload = createEmptyPayload();
    packet.write(payload);
    var wrapper = GenericPacketWrapper.newInstance(payload);
    assertTrue(wrapper.isEofPacket());
  }

  @Test
  public void testWrappedOtherPacket() {
    var payload = createEmptyPayload();
    var wrapper = GenericPacketWrapper.newInstance(payload);
    assertFalse(wrapper.isOkPacket());
    assertFalse(wrapper.isErrPacket());
    assertFalse(wrapper.isEofPacket());
    assertTrue(wrapper.isUnknownPacket());
  }
}
