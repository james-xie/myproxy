package com.gllue.transport.protocol.packet.query;

import static org.junit.Assert.assertEquals;

import com.gllue.transport.BaseTransportTest;
import com.gllue.transport.constant.MySQLColumnType;
import com.gllue.transport.constant.MySQLServerInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ColumnDefinition41PacketTest extends BaseTransportTest {
  @Test
  public void testReadWrite() {
    var packet =
        new ColumnDefinition41Packet(
            "def",
            "test db",
            "db table",
            "db table",
            "db column",
            "db column",
            MySQLServerInfo.DEFAULT_CHARSET,
            1000,
            MySQLColumnType.MYSQL_TYPE_BLOB.getValue(),
            0,
            0x00,
            "default values");
    var payload = createEmptyPayload();
    packet.write(payload);
    var newPacket = new ColumnDefinition41Packet(payload, true);

    assertEquals(packet.getCatalog(), newPacket.getCatalog());
    assertEquals(packet.getSchema(), newPacket.getSchema());
    assertEquals(packet.getTable(), newPacket.getTable());
    assertEquals(packet.getName(), newPacket.getName());
    assertEquals(packet.getCharset(), newPacket.getCharset());
    assertEquals(packet.getColumnLength(), newPacket.getColumnLength());
    assertEquals(packet.getColumnType(), newPacket.getColumnType());
    assertEquals(packet.getDecimals(), newPacket.getDecimals());
    assertEquals(packet.getFlags(), newPacket.getFlags());
    assertEquals(packet.getDefaultValues(), newPacket.getDefaultValues());
  }
}
