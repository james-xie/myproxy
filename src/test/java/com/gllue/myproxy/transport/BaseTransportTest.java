package com.gllue.myproxy.transport;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.gllue.myproxy.transport.exception.SQLErrorCode;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.OKPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.Before;

public class BaseTransportTest {
  private final List<MySQLPayload> createdPayloads = new LinkedList<>();

  @Before
  public void setUp() {

  }

  @After
  public void tearDown() {
    for (var payload: createdPayloads) {
      if (!payload.isClosed()) {
        payload.close();
      }
    }
  }

  protected MySQLPayload createEmptyPayload() {
    var byteBuf = Unpooled.buffer();
    var payload = new MySQLPayload(byteBuf);
    createdPayloads.add(payload);
    return payload;
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
