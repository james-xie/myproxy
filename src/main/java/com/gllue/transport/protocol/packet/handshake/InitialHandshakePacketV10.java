package com.gllue.transport.protocol.packet.handshake;

import com.gllue.transport.constant.MySQLCapabilityFlag;
import com.gllue.transport.constant.MySQLServerInfo;
import com.gllue.transport.constant.MySQLStatusFlag;
import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.payload.MySQLPayload;
import com.gllue.transport.exception.MalformedPacketException;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;
import lombok.Getter;

/**
 * MySQL Initial Handshake Packet - protocol version 10
 *
 * <pre>
 * @see <a href="https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake"></a>
 * </pre>
 */
@Getter
public class InitialHandshakePacketV10 implements MySQLPacket {
  private static final int PROTOCOL_VERSION = MySQLServerInfo.PROTOCOL_VERSION;
  private static final int DEFAULT_CHARSET = MySQLServerInfo.DEFAULT_CHARSET;
  private static final int DEFAULT_STATUS_FLAGS =
      MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT.getValue();
  private static final int AUTH_PLUGIN_DATA_PART_1_LENGTH = 8;
  private static final int AUTH_PLUGIN_DATA_PART_2_MIN_LENGTH = 12;

  private final String serverVersion;
  private final int connectionId;
  private final byte[] authPluginDataPart1;
  private final byte[] authPluginDataPart2;
  private final int capabilityFlags;
  private final int charset;
  private final int statusFlags;
  private final String authPluginName;

  public InitialHandshakePacketV10(
      String serverVersion,
      int connectionId,
      byte[] authPluginData,
      int capabilityFlags,
      int charset,
      int statusFlags,
      String authPluginName) {
    if (MySQLCapabilityFlag.CLIENT_SECURE_CONNECTION.isBitSet(capabilityFlags)) {
      Preconditions.checkArgument(
          authPluginData.length
              >= AUTH_PLUGIN_DATA_PART_1_LENGTH + AUTH_PLUGIN_DATA_PART_2_MIN_LENGTH);
    } else {
      Preconditions.checkArgument(authPluginData.length == AUTH_PLUGIN_DATA_PART_1_LENGTH);
    }
    if (MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.isBitSet(capabilityFlags)) {
      Preconditions.checkArgument(authPluginName != null);
    }

    this.serverVersion = serverVersion;
    this.connectionId = connectionId;
    this.authPluginDataPart1 = new byte[AUTH_PLUGIN_DATA_PART_1_LENGTH];
    this.authPluginDataPart2 = new byte[authPluginData.length - AUTH_PLUGIN_DATA_PART_1_LENGTH];
    System.arraycopy(authPluginData, 0, authPluginDataPart1, 0, authPluginDataPart1.length);
    System.arraycopy(
        authPluginData,
        authPluginDataPart1.length,
        authPluginDataPart2,
        0,
        authPluginDataPart2.length);

    this.capabilityFlags = capabilityFlags;
    this.charset = charset;
    this.statusFlags = statusFlags;
    this.authPluginName = authPluginName;
  }

  public InitialHandshakePacketV10(final MySQLPayload payload) {
    validateProtocolVersion(payload.readInt1());
    this.serverVersion = payload.readStringNul();
    this.connectionId = payload.readInt4();
    authPluginDataPart1 = payload.readStringFixReturnBytes(8);
    payload.skipBytes(1);
    int capabilityFlags = payload.readInt2();

    if (payload.readableBytes() < 6) {
      // no more data in the packet
      this.authPluginDataPart2 = new byte[0];
      this.capabilityFlags = capabilityFlags;
      this.charset = 0;
      this.statusFlags = 0;
      this.authPluginName = null;
      return;
    }

    int pluginDataLength = 0;

    this.charset = payload.readInt1();
    this.statusFlags = payload.readInt2();
    capabilityFlags |= payload.readInt2() << 16;
    this.capabilityFlags = capabilityFlags;

    if (MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.isBitSet(capabilityFlags)) {
      pluginDataLength = payload.readInt1();
    } else {
      payload.skipBytes(1);
    }

    // reserved
    payload.skipBytes(10);

    if (MySQLCapabilityFlag.CLIENT_SECURE_CONNECTION.isBitSet(capabilityFlags)) {
      // plugin data length includes auth_plugin_data_part_1 and filler
      authPluginDataPart2 = payload.readStringFixReturnBytes(Math.max(12, pluginDataLength - 9));
      payload.skipBytes(1);
    } else {
      authPluginDataPart2 = new byte[0];
    }

    if (MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.isBitSet(capabilityFlags)) {
      authPluginName = payload.readStringNul();
    } else {
      authPluginName = null;
    }
  }

  private void validateProtocolVersion(final int version) {
    if (version != PROTOCOL_VERSION) {
      throw new MalformedPacketException("Only protocol version 10 is supported.");
    }
  }

  @Override
  public void write(final MySQLPayload payload) {
    payload.writeInt1(PROTOCOL_VERSION);
    payload.writeStringNul(serverVersion);
    payload.writeInt4(connectionId);
    payload.writeBytes(authPluginDataPart1);
    payload.writeZero(1);
    payload.writeInt2(capabilityFlags & LOWER_2_BYTES_MASK);
    writeCharset(payload);
    writeStatusFlags(payload);
    payload.writeInt2((capabilityFlags >>> 16) & LOWER_2_BYTES_MASK);
    writeAuthPluginDataLength(payload);
    payload.writeZero(10);

    if (MySQLCapabilityFlag.CLIENT_SECURE_CONNECTION.isBitSet(capabilityFlags)) {
      payload.writeBytes(authPluginDataPart2);
      payload.writeZero(1);
    }

    if (MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.isBitSet(capabilityFlags)) {
      payload.writeStringNul(authPluginName);
    }
  }

  private void writeCharset(final MySQLPayload payload) {
    if (charset > 0) {
      payload.writeInt1(charset & LOWER_1_BYTES_MASK);
    } else {
      payload.writeInt1(DEFAULT_CHARSET);
    }
  }

  private void writeStatusFlags(final MySQLPayload payload) {
    if (statusFlags > 0) {
      payload.writeInt2(statusFlags & LOWER_2_BYTES_MASK);
    } else {
      payload.writeInt2(DEFAULT_STATUS_FLAGS);
    }
  }

  private void writeAuthPluginDataLength(final MySQLPayload payload) {
    if (MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.isBitSet(capabilityFlags)) {
      payload.writeInt1(authPluginDataPart1.length + authPluginDataPart2.length + 1);
    } else {
      payload.writeZero(1);
    }
  }

  public byte[] getAuthPluginData() {
    return Bytes.concat(authPluginDataPart1, authPluginDataPart2);
  }
}
