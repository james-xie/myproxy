package com.gllue.command.result;

import com.gllue.command.result.query.QueryResult;
import com.gllue.transport.protocol.packet.generic.OKPacket;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class CommandResult {
  private final long affectedRows;

  private final long lastInsertId;

  private final int statusFlag;

  private final int warnings;

  private final String info;

  private final QueryResult queryResult;

  public static CommandResult newInstance(final OKPacket packet) {
    return newInstance(packet, null);
  }

  public static CommandResult newInstance(final OKPacket packet, final QueryResult queryResult) {
    return new CommandResult(
        packet.getAffectedRows(),
        packet.getLastInsertId(),
        packet.getStatusFlag(),
        packet.getWarnings(),
        packet.getInfo(),
        queryResult);
  }
}
