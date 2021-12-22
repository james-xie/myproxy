package com.gllue.myproxy.transport.protocol.packet.handshake;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.transport.BaseTransportTest;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.OKPacket;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AuthPacketWrapperTest extends BaseTransportTest {
  @Test
  public void testWrappedErrPacket() {
    var packet = new ErrPacket(MySQLServerErrorCode.ER_NO_DB_ERROR);
    var payload = createEmptyPayload();
    packet.write(payload);
    var wrapper = AuthPacketWrapper.newInstance(payload);
    assertTrue(wrapper.isErrPacket());
  }

  @Test
  public void testWrappedAuthSwitchPacket() {
    var packet = new AuthSwitchRequestPacket("plugin name", "auth plugin data");
    var payload = createEmptyPayload();
    packet.write(payload);
    var wrapper = AuthPacketWrapper.newInstance(payload);
    assertThat(wrapper.getPacket(), instanceOf(AuthSwitchRequestPacket.class));
  }

  @Test
  public void testWrappedAuthMoreDataPacket() {
    var packet = new AuthMoreDataPacket("plugin data");
    var payload = createEmptyPayload();
    packet.write(payload);
    var wrapper = AuthPacketWrapper.newInstance(payload);
    assertThat(wrapper.getPacket(), instanceOf(AuthMoreDataPacket.class));
  }

  @Test
  public void testWrappedOkPacket() {
    var packet = new OKPacket();
    var payload = createEmptyPayload();
    packet.write(payload);
    var wrapper = AuthPacketWrapper.newInstance(payload);
    assertThat(wrapper.getPacket(), instanceOf(OKPacket.class));
  }
}
