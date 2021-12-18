package com.gllue.myproxy.bootstrap;

import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.config.ConfigurationException;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.GenericConfigProperties;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import com.gllue.myproxy.config.TransportConfigProperties;
import com.gllue.myproxy.transport.backend.connection.BackendConnectionFactory;
import com.gllue.myproxy.transport.backend.connection.ConnectionArguments;
import com.gllue.myproxy.transport.backend.datasource.BackendDataSource;
import com.gllue.myproxy.transport.backend.datasource.DataSourceConfig;
import com.gllue.myproxy.transport.core.service.TransportService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public final class ServerContext {
  private final Configurations configurations;
  private final ThreadPool threadPool;
  private final TransportService transportService;

  public static class Builder {
    private static final String PROPERTIES_FILE_NAME = "myproxy.properties";

    private final Configurations configurations;

    public Builder() {
      var properties = loadConfigurationProperties();
      this.configurations =
          new Configurations(
              List.of(
                  new GenericConfigProperties(properties),
                  new TransportConfigProperties(properties)));
    }

    private Properties loadConfigurationProperties() {
      var properties = new Properties();
      var stream = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME);
      if (stream == null) {
        throw new ConfigurationException(
            String.format("Properties file [%s] is not found.", PROPERTIES_FILE_NAME));
      }

      try {
        properties.load(stream);
      } catch (IOException e) {
        throw new ConfigurationException(
            String.format(
                "An exception is occurred when loading the properties file. [%s]",
                PROPERTIES_FILE_NAME));
      }
      return properties;
    }

    private ThreadPool newThreadPool() {
      return new ThreadPool(this.configurations);
    }

    private List<BackendDataSource> preloadDataSources() {
      List<String> dataSourceConfigs =
          this.configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.DATA_SOURCE_CONFIGS);
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

    private TransportService newTransportService() {
      return new TransportService(preloadDataSources());
    }

    public ServerContext build() {
      return new ServerContext(configurations, newThreadPool(), newTransportService());
    }
  }
}
