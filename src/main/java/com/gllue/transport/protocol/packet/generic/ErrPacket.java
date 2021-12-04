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

package com.gllue.transport.protocol.packet.generic;

import com.gllue.transport.exception.SQLErrorCode;
import com.gllue.transport.protocol.packet.MySQLPacket;
import com.gllue.transport.protocol.payload.MySQLPayload;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * ERR packet protocol for MySQL.
 *
 * @see <a href="https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html">ERR Packet</a>
 */
@RequiredArgsConstructor
@Getter
public final class ErrPacket implements MySQLPacket {

  /** Header of ERR packet. */
  public static final int HEADER = 0xff;

  private static final String SQL_STATE_MARKER = "#";

  private final int errorCode;

  private final String sqlState;

  private final String errorMessage;

  public ErrPacket(final SQLErrorCode sqlErrorCode, final Object... errorMessageArguments) {
    this(
        sqlErrorCode.getErrorCode(),
        sqlErrorCode.getSqlState(),
        String.format(sqlErrorCode.getErrorMessage(), errorMessageArguments));
  }

  public ErrPacket(final MySQLPayload payload) {
    Preconditions.checkArgument(
        HEADER == payload.readInt1(), "Header of MySQL ERR packet must be `0xff`.");
    errorCode = payload.readInt2();
    payload.readStringFix(1);
    sqlState = payload.readStringFix(5);
    errorMessage = payload.readStringEOF();
  }

  @Override
  public void write(final MySQLPayload payload) {
    payload.writeInt1(HEADER);
    payload.writeInt2(errorCode);
    payload.writeStringFix(SQL_STATE_MARKER);
    payload.writeStringFix(sqlState);
    payload.writeStringEOF(errorMessage);
  }

  public static boolean match(final MySQLPayload payload) {
    return payload.readableBytes() > 0 && payload.peek() == HEADER;
  }

  @Override
  public String toString() {
    return String.format("ErrPacket[%s:%s]", errorCode, errorMessage);
  }
}
