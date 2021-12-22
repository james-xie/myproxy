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
import com.gllue.myproxy.transport.protocol.packet.MySQLPacket;
import com.gllue.myproxy.transport.protocol.payload.MySQLPayload;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * OK packet protocol for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html">OK Packet</a>
 */
@RequiredArgsConstructor
@Getter
public final class OKPacket implements MySQLPacket {

  /** Header of OK packet. */
  public static final int HEADER = 0x00;

  private static final int DEFAULT_STATUS_FLAG =
      MySQLStatusFlag.SERVER_STATUS_AUTOCOMMIT.getValue();

  private final long affectedRows;

  private final long lastInsertId;

  private final int statusFlag;

  private final int warnings;

  private final String info;

  public OKPacket() {
    this(0L, 0L, DEFAULT_STATUS_FLAG, 0, "");
  }

  public OKPacket(final long affectedRows, final long lastInsertId) {
    this(affectedRows, lastInsertId, DEFAULT_STATUS_FLAG, 0, "");
  }

  public OKPacket(final MySQLPayload payload) {
    Preconditions.checkArgument(
        HEADER == payload.readInt1(), "Header of MySQL OK packet must be `0x00`.");
    affectedRows = payload.readIntLenenc();
    lastInsertId = payload.readIntLenenc();
    statusFlag = payload.readInt2();
    warnings = payload.readInt2();
    info = payload.readStringEOF();
  }

  @Override
  public void write(final MySQLPayload payload) {
    payload.writeInt1(HEADER);
    payload.writeIntLenenc(affectedRows);
    payload.writeIntLenenc(lastInsertId);
    payload.writeInt2(statusFlag);
    payload.writeInt2(warnings);
    payload.writeStringEOF(info);
  }

  public static boolean match(final MySQLPayload payload) {
    return payload.readableBytes() >= 7 && payload.peek() == OKPacket.HEADER;
  }

}
