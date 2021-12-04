package com.gllue.transport.protocol.packet.handshake;

import com.gllue.transport.constant.MySQLCapabilityFlag;
import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.payload.MySQLPayload;
import com.gllue.transport.exception.MalformedPacketException;
import com.google.common.base.Preconditions;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;

@Getter
public class HandshakeResponsePacket41 implements MySQLPacket {

  private final int clientCapabilityFlags;
  private final int maxPacketSize;
  private final int charset;
  private final String username;
  private final byte[] authResponse;
  private final String database;
  private final String authPluginName;
  private final Map<String, String> connectAttrs = new HashMap<>();

  public HandshakeResponsePacket41(
      int clientCapabilityFlags,
      int maxPacketSize,
      int charset,
      String username,
      byte[] authResponse,
      String database,
      String authPluginName) {
    if (MySQLCapabilityFlag.CLIENT_CONNECT_WITH_DB.isBitSet(clientCapabilityFlags)) {
      Preconditions.checkArgument(database != null && !database.isEmpty());
    }
    if (MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.isBitSet(clientCapabilityFlags)) {
      Preconditions.checkArgument(authPluginName != null && !authPluginName.isEmpty());
    }

    this.clientCapabilityFlags = clientCapabilityFlags;
    this.maxPacketSize = maxPacketSize;
    this.charset = charset;
    this.username = username;
    this.authResponse = authResponse;
    this.database = database;
    this.authPluginName = authPluginName;
  }

  public HandshakeResponsePacket41(final MySQLPayload payload) {
    clientCapabilityFlags = payload.readInt4();
    maxPacketSize = payload.readInt4();
    charset = payload.readInt1();
    payload.skipBytes(23);
    username = payload.readStringNul();
    authResponse = readAuthResponse(payload);

    if (MySQLCapabilityFlag.CLIENT_CONNECT_WITH_DB.isBitSet(clientCapabilityFlags)) {
      database = payload.readStringNul();
    } else {
      database = null;
    }

    if (MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.isBitSet(clientCapabilityFlags)) {
      authPluginName = payload.readStringNul();
    } else {
      authPluginName = null;
    }

    readConnectAttrs(payload);
  }

  private byte[] readAuthResponse(final MySQLPayload payload) {
    byte[] authResponse;
    if (MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.isBitSet(clientCapabilityFlags)) {
      var authRespLen = (int) payload.readIntLenenc();
      authResponse = payload.readStringFixReturnBytes(authRespLen);
    } else if (MySQLCapabilityFlag.CLIENT_SECURE_CONNECTION.isBitSet(clientCapabilityFlags)) {
      var authRespLen = payload.readInt1();
      authResponse = payload.readStringFixReturnBytes(authRespLen);
    } else {
      authResponse = payload.readStringNulReturnBytes();
    }
    return authResponse;
  }

  private void readConnectAttrs(final MySQLPayload payload) {
    if (MySQLCapabilityFlag.CLIENT_CONNECT_ATTRS.isBitSet(clientCapabilityFlags)) {
      long totalLength = payload.readIntLenenc();
      long readableBytes = payload.readableBytes();
      while (totalLength > 0) {
        var key = payload.readStringLenenc();
        var value = payload.readStringLenenc();
        connectAttrs.putIfAbsent(key, value);
        totalLength -= readableBytes - payload.readableBytes();
        readableBytes = payload.readableBytes();
      }
      if (totalLength != 0) {
        throw new MalformedPacketException("Failed to resolve client connect attributes.");
      }
    }
  }

  public void addConnectAttribute(final String key, final String value) {
    connectAttrs.putIfAbsent(key, value);
  }

  @Override
  public void write(final MySQLPayload payload) {
    payload.writeInt4(clientCapabilityFlags);
    payload.writeInt4(maxPacketSize);
    payload.writeInt1(charset);
    payload.writeZero(23);
    payload.writeStringNul(username);
    writeAuthResponse(payload);

    if (MySQLCapabilityFlag.CLIENT_CONNECT_WITH_DB.isBitSet(clientCapabilityFlags)) {
      payload.writeStringNul(database);
    }

    if (MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.isBitSet(clientCapabilityFlags)) {
      payload.writeStringNul(authPluginName);
    }

    if (MySQLCapabilityFlag.CLIENT_CONNECT_ATTRS.isBitSet(clientCapabilityFlags)) {
      writeConnectAttrs(payload);
    }
  }

  private void writeAuthResponse(final MySQLPayload payload) {
    if (MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.isBitSet(clientCapabilityFlags)) {
      payload.writeIntLenenc(authResponse.length);
      payload.writeBytes(authResponse);
    } else if (MySQLCapabilityFlag.CLIENT_SECURE_CONNECTION.isBitSet(clientCapabilityFlags)) {
      if (authResponse.length > 0xff) {
        throw new MalformedPacketException("Auth response size is too large.");
      }
      payload.writeInt1(authResponse.length);
      payload.writeBytes(authResponse);
    } else {
      payload.writeBytes(authResponse);
      payload.writeZero(1);
    }
  }

  private void writeConnectAttrs(final MySQLPayload payload) {
    // Compute the total length of all key-values
    int length = 0;
    for (var entry : connectAttrs.entrySet()) {
      var keyLen = entry.getKey().length();
      var valLen = entry.getValue().length();
      length += keyLen + valLen;
      length += MySQLPayload.getLenencIntOccupiedBytes(keyLen);
      length += MySQLPayload.getLenencIntOccupiedBytes(valLen);
    }

    payload.writeIntLenenc(length);
    for (var entry : connectAttrs.entrySet()) {
      payload.writeStringLenenc(entry.getKey());
      payload.writeStringLenenc(entry.getValue());
    }
  }

  public boolean isClientPluginAuth() {
    return MySQLCapabilityFlag.CLIENT_PLUGIN_AUTH.isBitSet(clientCapabilityFlags);
  }
}
