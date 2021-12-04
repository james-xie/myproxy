package com.gllue.metadata.command.context;

import com.gllue.config.Configurations;
import com.gllue.metadata.MetaData;
import com.gllue.repository.PersistRepository;

public interface CommandExecutionContext<T extends MetaData> {
  T getRootMetaData();

  PersistRepository getRepository();

  Configurations getConfigurations();
}
