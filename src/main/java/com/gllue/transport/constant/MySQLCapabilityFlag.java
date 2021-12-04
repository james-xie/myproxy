package com.gllue.transport.constant;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Capability flag for MySQL.
 *
 * @see <a
 *     href="https://dev.mysql.com/doc/internals/en/capability-flags.html#packet-Protocol::CapabilityFlags">CapabilityFlags</a>
 */
@RequiredArgsConstructor
@Getter
public enum MySQLCapabilityFlag {
  CLIENT_LONG_PASSWORD(0x00000001),

  CLIENT_FOUND_ROWS(0x00000002),

  CLIENT_LONG_FLAG(0x00000004),

  CLIENT_CONNECT_WITH_DB(0x00000008),

  CLIENT_NO_SCHEMA(0x00000010),

  CLIENT_COMPRESS(0x00000020),

  CLIENT_ODBC(0x00000040),

  CLIENT_LOCAL_FILES(0x00000080),

  CLIENT_IGNORE_SPACE(0x00000100),

  CLIENT_PROTOCOL_41(0x00000200),

  CLIENT_INTERACTIVE(0x00000400),

  CLIENT_SSL(0x00000800),

  CLIENT_IGNORE_SIGPIPE(0x00001000),

  CLIENT_TRANSACTIONS(0x00002000),

  CLIENT_RESERVED(0x00004000),

  CLIENT_SECURE_CONNECTION(0x00008000),

  CLIENT_MULTI_STATEMENTS(0x00010000),

  CLIENT_MULTI_RESULTS(0x00020000),

  CLIENT_PS_MULTI_RESULTS(0x00040000),

  CLIENT_PLUGIN_AUTH(0x00080000),

  CLIENT_CONNECT_ATTRS(0x00100000),

  CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA(0x00200000),

  CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS(0x00400000),

  CLIENT_SESSION_TRACK(0x00800000),

  CLIENT_DEPRECATE_EOF(0x01000000);

  private final int value;

  /**
   * Get handshake server capability flags.
   *
   * @return handshake capability flags
   */
  public static int handshakeServerCapabilityFlags() {
    return unionCapabilityFlags(
        CLIENT_LONG_PASSWORD,
        CLIENT_FOUND_ROWS,
        CLIENT_LONG_FLAG,
        CLIENT_CONNECT_WITH_DB,
        CLIENT_ODBC,
        CLIENT_IGNORE_SPACE,
        CLIENT_PROTOCOL_41,
        CLIENT_INTERACTIVE,
        CLIENT_IGNORE_SIGPIPE,
        CLIENT_TRANSACTIONS,
        CLIENT_SECURE_CONNECTION,
        CLIENT_PLUGIN_AUTH,
        CLIENT_CONNECT_ATTRS);
  }

  /**
   * Get handshake client capability flags.
   *
   * @return handshake capability flags
   */
  public static int handshakeClientCapabilityFlags(final int serverCapabilityFlags) {
    int result =
        unionCapabilityFlags(
            CLIENT_LONG_PASSWORD, CLIENT_LONG_FLAG, CLIENT_PROTOCOL_41, CLIENT_TRANSACTIONS);

    // The client should only announce the capabilities in the Handshake Response Packet
    // that it has in common with the server.
    var flags =
        new MySQLCapabilityFlag[] {
          CLIENT_PLUGIN_AUTH,
          CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA,
          CLIENT_CONNECT_ATTRS,
          CLIENT_CONNECT_WITH_DB
        };

    for (var each : flags) {
      result |= each.value & serverCapabilityFlags;
    }
    return result;
  }

  /**
   * Union capability flags.
   *
   * @param capabilities single capabilities of need to be calculated
   * @return unioned capabilities
   */
  public static int unionCapabilityFlags(final MySQLCapabilityFlag... capabilities) {
    int result = 0;
    for (MySQLCapabilityFlag each : capabilities) {
      result |= each.value;
    }
    return result;
  }

  public boolean isBitSet(int capabilityFlags) {
    return (capabilityFlags & value) > 0;
  }
}
