package com.gllue.myproxy.transport.core.service;

import com.gllue.myproxy.bootstrap.ServerContext;
import com.gllue.myproxy.common.Initializer;
import com.gllue.myproxy.config.ConfigurationException;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import com.gllue.myproxy.transport.backend.connection.BackendConnectionFactory;
import com.gllue.myproxy.transport.backend.datasource.BackendDataSource;
import com.gllue.myproxy.transport.backend.datasource.DataSourceConfig;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TransportServiceInitializer implements Initializer {
  private TransportService transportService;

  private List<BackendDataSource> preloadDataSources(
      Configurations configurations, TransportService transportService) {
    List<String> dataSourceConfigs =
        configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.DATA_SOURCE_CONFIGS);
    if (dataSourceConfigs.size() == 0) {
      throw new ConfigurationException("Missing data source configuration.");
    }

    var backendConnectionFactory = new BackendConnectionFactory(transportService);
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

      var dataSource =
          new BackendDataSource(
              name,
              configObject.getAddress(),
              configObject.getUser(),
              configObject.getPassword(),
              backendConnectionFactory);
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
    transportService =
        new TransportService(context.getConfigurations(), context.getThreadPool());
    var dataSources = preloadDataSources(context.getConfigurations(), transportService);
    transportService.initialize(dataSources);
    context.setTransportService(transportService);
  }

  @Override
  public void close() throws Exception {
    if (transportService != null) transportService.close();
  }
}
