package com.gllue.myproxy.transport.protocol.packet;

import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;

public interface MySQLPacket {

  int MAX_PACKET_SIZE = (1 << 24) - 1;

  int LOWER_1_BYTES_MASK = 0x000000ff;

  int LOWER_2_BYTES_MASK = 0x0000ffff;

  int UPPER_2_BYTES_MASK = 0xffff0000;

  int NULL = 0xfb;

  /**
   * Write packet to byte buffer.
   *
   * @param payload packet payload to be written
   */
  void write(final MySQLPayload payload);
}
