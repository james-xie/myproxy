package com.gllue.myproxy.transport.protocol.packet.query;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.transport.BaseTransportTest;
import com.gllue.myproxy.transport.constant.MySQLColumnType;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.constant.MySQLServerInfo;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ColumnDefinitionPacketWrapperTest extends BaseTransportTest {
  @Test
  public void testWrappedColumnDefinitionPacket() {
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
    var wrapper = ColumnDefinitionPacketWrapper.newInstance(payload, false);
    assertNotNull(wrapper);
    assertFalse(wrapper.isErrPacket());
    assertThat(wrapper.getPacket(), instanceOf(ColumnDefinition41Packet.class));
  }

  @Test
  public void testWrappedErrPacket() {
    var packet = new ErrPacket(MySQLServerErrorCode.ER_NO_DB_ERROR);
    var payload = createEmptyPayload();
    packet.write(payload);
    var wrapper = ColumnDefinitionPacketWrapper.newInstance(payload, false);
    assertNotNull(wrapper);
    assertTrue(wrapper.isErrPacket());
  }
}
