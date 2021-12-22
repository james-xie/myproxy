package com.gllue.transport.core.service;

import com.gllue.bootstrap.ServerContext;
import com.gllue.common.Initializer;
import com.gllue.config.ConfigurationException;
import com.gllue.config.Configurations;
import com.gllue.config.Configurations.Type;
import com.gllue.config.GenericConfigPropertyKey;
import com.gllue.transport.backend.connection.BackendConnectionFactory;
import com.gllue.transport.backend.connection.ConnectionArguments;
import com.gllue.transport.backend.datasource.BackendDataSource;
import com.gllue.transport.backend.datasource.DataSourceConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TransportServiceInitializer implements Initializer {

  private List<BackendDataSource> preloadDataSources(Configurations configurations) {
    List<String> dataSourceConfigs =
        configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.DATA_SOURCE_CONFIGS);
    if (dataSourceConfigs.size() == 0) {
      throw new ConfigurationException("Missing data source configuration.");
    }

    var backendConnectionFactory = new BackendConnectionFactory();
    var parser = new DataSourceConfig.Parser();
    Set<String> nameSet = new HashSet<>();
    List<BackendDataSource> dataSources = new ArrayList<>();
    for (var config : dataSourceConfigs) {
      var configObject = parser.parse(config);
      var name = configObject.getName();
      if (nameSet.contains(name)) {
        throw new ConfigurationException(
            String.format("Data source name must be an unique string. [%s]", name));
      }
      nameSet.add(name);

      var connectionArgs =
          new ConnectionArguments(
              configObject.getAddress(),
              configObject.getUser(),
              configObject.getPassword(),
              configObject.getDatabase());

      var dataSource = new BackendDataSource(name, 500, backendConnectionFactory, connectionArgs);
      dataSources.add(dataSource);
    }
    return dataSources;
  }

  @Override
  public String name() {
    return "transport service";
  }

  @Override
  public void initialize(ServerContext context) {
    var transportService = new TransportService(preloadDataSources(context.getConfigurations()));
    context.setTransportService(transportService);
  }

  @Override
  public void close() throws Exception {}
}
