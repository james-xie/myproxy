package com.gllue.myproxy.metadata.command.context;

import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.metadata.MetaData;
import com.gllue.myproxy.repository.PersistRepository;

public interface CommandExecutionContext<T extends MetaData> {
  T getRootMetaData();

  PersistRepository getRepository();

  Configurations getConfigurations();
}
