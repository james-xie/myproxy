package com.gllue.myproxy.transport.protocol.packet.query;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.transport.BaseTransportTest;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.protocol.packet.query.text.TextResultSetRowPacket;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TextResultSetRowPacketWrapperTest extends BaseTransportTest {
  @Test
  public void testWrappedTextResultSetRowPacket() {
    List<String> data = new ArrayList<>(List.of("abc", "efg", "hijk", "1243"));
    var packet = new TextResultSetRowPacket(data.toArray(new String[0]));
    var payload = createEmptyPayload();
    packet.write(payload);
    var wrapper = TextResultSetRowPacketWrapper.newInstance(payload, data.size());
    assertNotNull(wrapper);
    assertFalse(wrapper.isErrPacket());
    assertThat(wrapper.getPacket(), instanceOf(TextResultSetRowPacket.class));
  }

  @Test
  public void testWrappedErrPacket() {
    var packet = new ErrPacket(MySQLServerErrorCode.ER_NO_DB_ERROR);
    var payload = createEmptyPayload();
    packet.write(payload);
    var wrapper = TextResultSetRowPacketWrapper.newInstance(payload, 1);
    assertNotNull(wrapper);
    assertTrue(wrapper.isErrPacket());
  }
}
