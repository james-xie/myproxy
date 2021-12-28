package com.gllue.myproxy.transport.protocol.packet.command;

import com.gllue.myproxy.transport.BaseTransportTest;
import com.gllue.myproxy.transport.constant.MySQLCommandPacketType;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SimpleCommandPacketTest extends BaseTransportTest {
  @Test
  public void testSimpleCommand() {
    var packet = new SimpleCommandPacket(MySQLCommandPacketType.COM_SLEEP);
    Assert.assertEquals(MySQLCommandPacketType.COM_SLEEP, packet.getCommandType());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBadSimpleCommand() {
    new SimpleCommandPacket(MySQLCommandPacketType.COM_QUERY);
  }
}
