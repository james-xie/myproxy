package com.gllue.metadata.command.context;

import com.gllue.config.Configurations;
import com.gllue.metadata.model.MultiDatabasesMetaData;
import com.gllue.repository.PersistRepository;
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
