package com.gllue.myproxy.transport.protocol.packet.query.text;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.gllue.myproxy.transport.BaseTransportTest;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TextResultSetRowPacketTest extends BaseTransportTest {
  @Test
  public void testReadWrite() {
    List<String> data = new ArrayList<>(List.of("abc", "efg", "hijk", "1243"));
    data.add(null);
    data.add(null);
    var packet = new TextResultSetRowPacket(data.toArray(new String[0]));
    var payload = createEmptyPayload();
    packet.write(payload);
    var newPacket = new TextResultSetRowPacket(payload, data.size());
    assertArrayEquals(packet.getRowData(), newPacket.getRowData());
  }
}
