package com.gllue.metadata.command;

import com.gllue.metadata.MetaData;
import com.gllue.metadata.command.context.CommandExecutionContext;

public interface MetaDataCommand<T extends MetaData> {
  void execute(CommandExecutionContext<T> context);
}
