package com.gllue.myproxy.metadata.command;

import com.gllue.myproxy.metadata.MetaData;
import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;

public interface MetaDataCommand<T extends MetaData> {
  void execute(CommandExecutionContext<T> context);
}
