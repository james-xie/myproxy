/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.gllue.myproxy.transport.protocol.packet.generic;

import com.gllue.myproxy.transport.constant.MySQLStatusFlag;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * EOF packet protocol for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html">EOF Packet</a>
 */
@RequiredArgsConstructor
@Getter
public final class EofPacket implements MySQLPacket {

  /** Header of EOF packet. */
  public static final int HEADER = 0xfe;

  private final int warnings;

  private final int statusFlags;

  public EofPacket() {
    this(0, MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT.getValue());
  }

  public EofPacket(final MySQLPayload payload) {
    Preconditions.checkArgument(
        HEADER == payload.readInt1(), "Header of MySQL EOF packet must be `0xfe`.");
    warnings = payload.readInt2();
    statusFlags = payload.readInt2();
  }

  @Override
  public void write(final MySQLPayload payload) {
    payload.writeInt1(HEADER);
    payload.writeInt2(warnings);
    payload.writeInt2(statusFlags);
  }

  /**
   * <pre>
   * @see <a href="https://dev.mysql.com/doc/internals/en/packet-EOF_Packet.html">EOF_Packet</a>
   * </pre>
   *
   * @param payload MySQL payload
   * @return true if the given payload is a EofPacket, otherwise false.
   */
  public static boolean match(final MySQLPayload payload) {
    // The EOF packet may appear in places where a Protocol::LengthEncodedInteger may appear. You
    // must check whether the packet length is less than 9 to make sure that it is a EOF packet.
    return payload.readableBytes() > 0 && payload.readableBytes() < 9 && payload.peek() == HEADER;
  }
}
