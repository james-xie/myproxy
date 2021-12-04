package com.gllue.transport.core.connection;

import com.gllue.transport.backend.connection.BackendConnectionImpl;
import com.gllue.transport.frontend.connection.FrontendConnectionImpl;
import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.packet.generic.OKPacket;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AdaptableTrafficThrottlePipeTest {
  EmbeddedChannel inChannel = new EmbeddedChannel();
  EmbeddedChannel outChannel = new EmbeddedChannel();
  Connection in = new BackendConnectionImpl(1, inChannel);
  Connection out = new FrontendConnectionImpl(2, outChannel, "datasource");

  @Test
  public void testTransfer() throws Exception {
    var pipe = new AdaptableTrafficThrottlePipe(in, out);
    pipe.prepareToTransfer();

    MySQLPacket[] writePackets = new MySQLPacket[100];
    for (int i=0; i<100; i++) {
      var writePacket = new OKPacket();
      writePackets[i] = writePacket;
      pipe.transfer(writePacket, false);
    }

    outChannel.flush();

    for (int i=0; i<100; i++) {
      Assert.assertEquals(writePackets[i], outChannel.readOutbound());
    }

    pipe.close();
    Assert.assertTrue(in.isAutoRead());
  }
}
