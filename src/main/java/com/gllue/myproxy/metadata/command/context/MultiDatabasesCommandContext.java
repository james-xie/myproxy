package com.gllue.myproxy.metadata.command.context;

import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.repository.PersistRepository;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class MultiDatabasesCommandContext
    implements CommandExecutionContext<MultiDatabasesMetaData> {
  private final MultiDatabasesMetaData metaData;
  private final PersistRepository repository;
  private final Configurations configurations;

  @Override
  public MultiDatabasesMetaData getRootMetaData() {
    return metaData;
  }

  @Override
  public PersistRepository getRepository() {
    return repository;
  }

  @Override
  public Configurations getConfigurations() {
    return configurations;
  }
}
