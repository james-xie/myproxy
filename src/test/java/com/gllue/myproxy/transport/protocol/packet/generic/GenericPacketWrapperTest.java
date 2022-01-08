package com.gllue.myproxy.transport.protocol.packet.generic;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.transport.BaseTransportTest;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

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
