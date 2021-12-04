package com.gllue.transport;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.gllue.transport.exception.SQLErrorCode;
import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.packet.generic.ErrPacket;
import com.gllue.transport.protocol.packet.generic.OKPacket;
import com.gllue.transport.protocol.payload.MySQLPayload;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class BaseTransportTest {
  protected MySQLPayload createEmptyPayload() {
    var byteBuf = Unpooled.buffer();
    return new MySQLPayload(byteBuf);
  }

  protected MySQLPayload packetToPayload(MySQLPacket packet) {
    var payload = createEmptyPayload();
    packet.write(payload);
    return payload;
  }

  protected byte[] transformByteBufToBytes(ByteBuf buf) {
    byte[] bytes = new byte[buf.readableBytes()];
    buf.readBytes(bytes);
    return bytes;
  }

  protected byte[] transformByteBufToBytes(ByteBuf buf, int length) {
    byte[] bytes = new byte[length];
    buf.readBytes(bytes, 0, length);
    return bytes;
  }

  protected void assertOkPacket(MySQLPacket packet) {
    assertNotNull(packet);
    assertThat(packet, instanceOf(OKPacket.class));
  }

  protected void assertErrorPacket(final MySQLPacket packet) {
    assertNotNull(packet);
    assertThat(packet, instanceOf(ErrPacket.class));
  }

  protected void assertErrorPacket(final MySQLPacket packet, final SQLErrorCode errorCode) {
    assertNotNull(packet);
    assertThat(packet, instanceOf(ErrPacket.class));
    var errPacket = (ErrPacket) packet;
    assertEquals(errorCode.getErrorCode(), errPacket.getErrorCode());
  }

  protected void assertPayloadClosed(final MySQLPayload payload) {
    assertEquals(0, payload.getByteBuf().refCnt());
  }
}
