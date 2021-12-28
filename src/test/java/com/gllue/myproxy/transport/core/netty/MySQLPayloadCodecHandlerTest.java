package com.gllue.myproxy.transport.core.netty;

import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.transport.BaseTransportTest;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.google.common.primitives.Bytes;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MySQLPayloadCodecHandlerTest extends BaseTransportTest {
  @Test
  public void testSimpleDecode() {
    EmbeddedChannel ch = new EmbeddedChannel(new MySQLPayloadCodecHandler());

    for (int i = 0; i < 5; i++) {
      ByteBuf buffer = Unpooled.buffer();
      var payload = "decode payload".getBytes();
      buffer.writeMediumLE(payload.length);
      buffer.writeByte(0);
      buffer.writeBytes(payload);

      ch.writeInbound(buffer);

      var message = (MySQLPayload) ch.readInbound();
      var byteBuf = message.getByteBuf();
      Assert.assertEquals(payload.length, byteBuf.readableBytes());
      Assert.assertArrayEquals(payload, transformByteBufToBytes(byteBuf));
      message.close();
    }

    ch.finish();
  }

  @Test
  public void testTwoPhaseDecode() {
    EmbeddedChannel ch = new EmbeddedChannel(new MySQLPayloadCodecHandler());

    ByteBuf buffer = Unpooled.buffer();
    var payload = "decode payload".getBytes();
    buffer.writeMediumLE(payload.length);
    buffer.writeByte(0);
    buffer.writeBytes(payload);

    ch.writeInbound(buffer.readRetainedSlice(payload.length - 5));
    Assert.assertNull(ch.readInbound());

    ch.writeInbound(buffer);

    var message = (MySQLPayload) ch.readInbound();
    var byteBuf = message.getByteBuf();
    Assert.assertEquals(payload.length, byteBuf.readableBytes());
    Assert.assertArrayEquals(payload, transformByteBufToBytes(byteBuf));

    message.close();
    ch.finish();
  }

  @Test
  public void testDecodeWithCompositeMultiPayload1() {
    EmbeddedChannel ch = new EmbeddedChannel(new MySQLPayloadCodecHandler());

    var buffer1 = Unpooled.buffer();
    var payload1 = RandomUtils.generateRandomBytes(MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE);
    buffer1.writeMediumLE(payload1.length);
    buffer1.writeByte(0);
    buffer1.writeBytes(payload1);
    ch.writeInbound(buffer1);
    Assert.assertNull(ch.readInbound());

    var buffer2 = Unpooled.buffer();
    var payload2 = RandomUtils.generateRandomBytes(MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE);
    buffer2.writeMediumLE(payload2.length);
    buffer2.writeByte(1);
    buffer2.writeBytes(payload2);
    ch.writeInbound(buffer2);
    Assert.assertNull(ch.readInbound());

    var buffer3 = Unpooled.buffer();
    var payload3 = RandomUtils.generateRandomBytes(MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE - 1);
    buffer3.writeMediumLE(payload3.length);
    buffer3.writeByte(2);
    buffer3.writeBytes(payload3);
    ch.writeInbound(buffer3);

    var message = (MySQLPayload) ch.readInbound();
    var byteBuf = message.getByteBuf();
    Assert.assertNotNull(message);
    Assert.assertEquals(
        payload1.length + payload2.length + payload3.length, message.readableBytes());
    Assert.assertArrayEquals(
        Bytes.concat(payload1, payload2, payload3), transformByteBufToBytes(byteBuf));
    message.close();
    ch.finish();
  }

  @Test
  public void testDecodeWithCompositeMultiPayload2() {
    EmbeddedChannel ch = new EmbeddedChannel(new MySQLPayloadCodecHandler());

    var buffer1 = Unpooled.buffer();
    var payload1 = RandomUtils.generateRandomBytes(MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE);
    buffer1.writeMediumLE(payload1.length);
    buffer1.writeByte(0);
    buffer1.writeBytes(payload1);
    ch.writeInbound(buffer1);
    Assert.assertNull(ch.readInbound());

    var buffer2 = Unpooled.buffer();
    buffer2.writeMediumLE(0);
    buffer2.writeByte(1);
    buffer2.writeBytes(new byte[0]);
    ch.writeInbound(buffer2);

    var message = (MySQLPayload) ch.readInbound();
    Assert.assertNotNull(message);
    Assert.assertEquals(payload1.length, message.readableBytes());

    message.close();
    ch.finish();
  }

  @Test
  public void testDecodeWithBadSequenceId() {
    EmbeddedChannel ch = new EmbeddedChannel(new MySQLPayloadCodecHandler());

    var buffer1 = Unpooled.buffer();
    var payload1 = RandomUtils.generateRandomBytes(MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE);
    buffer1.writeMediumLE(payload1.length);
    buffer1.writeByte(0);
    buffer1.writeBytes(payload1);
    ch.writeInbound(buffer1);
    Assert.assertNull(ch.readInbound());

    var buffer2 = Unpooled.buffer();
    buffer2.writeMediumLE(0);
    buffer2.writeByte(3);
    buffer2.writeBytes(new byte[0]);
    ch.writeInbound(buffer2);

    Assert.assertFalse(ch.isOpen());
    ch.finish();
  }

  private MySQLPacket preparePacket(byte[] array) {
    return (payload) -> payload.writeBytes(array);
  }

  @Test
  public void testSimpleEncode() {
    EmbeddedChannel ch = new EmbeddedChannel(new MySQLPayloadCodecHandler());

    for (int i = 0; i < 5; i++) {
      byte[] array = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
      ch.writeOutbound(preparePacket(array));
      var outbound = (ByteBuf) ch.readOutbound();
      Assert.assertNotNull(outbound);

      int payloadLength = outbound.readUnsignedMediumLE();
      Assert.assertEquals(array.length, payloadLength);
      Assert.assertEquals(i, outbound.readByte());
      Assert.assertArrayEquals(array, transformByteBufToBytes(outbound));

      outbound.release();
    }

    ch.finish();
  }

  @Test
  public void testEncodeBigPacket() {
    EmbeddedChannel ch = new EmbeddedChannel(new MySQLPayloadCodecHandler());

    byte[] array = RandomUtils.generateRandomBytes(MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE * 2);
    ch.writeOutbound(preparePacket(array));

    var outbound = (ByteBuf) ch.readOutbound();
    Assert.assertNotNull(outbound);
    var headerSize = 4;
    Assert.assertEquals(array.length + headerSize * 3, outbound.readableBytes());

    int payloadLength = outbound.readUnsignedMediumLE();
    Assert.assertEquals(MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE, payloadLength);
    Assert.assertEquals(0, outbound.readByte());
    byte[] part1 = new byte[MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE];
    System.arraycopy(array, 0, part1, 0, MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE);
    Assert.assertArrayEquals(
        part1, transformByteBufToBytes(outbound, MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE));

    payloadLength = outbound.readUnsignedMediumLE();
    Assert.assertEquals(MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE, payloadLength);
    Assert.assertEquals(1, outbound.readByte());
    byte[] part2 = new byte[MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE];
    System.arraycopy(
        array,
        MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE,
        part2,
        0,
        MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE);
    Assert.assertArrayEquals(
        part2, transformByteBufToBytes(outbound, MySQLPayloadCodecHandler.MAX_PAYLOAD_SIZE));

    payloadLength = outbound.readUnsignedMediumLE();
    Assert.assertEquals(0, payloadLength);
    Assert.assertEquals(2, outbound.readByte());

    outbound.release();
    ch.finish();
  }
}
