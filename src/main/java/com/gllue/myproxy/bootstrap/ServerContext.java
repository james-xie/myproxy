package com.gllue.myproxy.bootstrap;

import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.generator.IdGenerator;
import com.gllue.myproxy.config.ConfigurationException;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.GenericConfigProperties;
import com.gllue.myproxy.config.TransportConfigProperties;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.core.service.TransportService;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public final class ServerContext {
  private final Properties properties;
  private final Configurations configurations;
  private final SQLParser sqlParser;

  private ThreadPool threadPool;
  private TransportService transportService;
  private PersistRepository persistRepository;
  private ClusterState clusterState;
  private IdGenerator idGenerator;

  public void setThreadPool(ThreadPool threadPool) {
    Preconditions.checkArgument(threadPool != null);
    Preconditions.checkState(this.threadPool == null);
    this.threadPool = threadPool;
  }

  public void setTransportService(TransportService transportService) {
    Preconditions.checkArgument(transportService != null);
    Preconditions.checkState(this.transportService == null);
    this.transportService = transportService;
  }

  public void setPersistRepository(PersistRepository persistRepository) {
    Preconditions.checkArgument(persistRepository != null);
    Preconditions.checkState(this.persistRepository == null);
    this.persistRepository = persistRepository;
  }

  public void setClusterState(ClusterState clusterState) {
    Preconditions.checkArgument(clusterState != null);
    Preconditions.checkState(this.clusterState == null);
    this.clusterState = clusterState;
  }

  public void setIdGenerator(IdGenerator idGenerator) {
    Preconditions.checkArgument(idGenerator != null);
    Preconditions.checkState(this.idGenerator == null);
    this.idGenerator = idGenerator;
  }

  public Properties getProperties() {
    Preconditions.checkNotNull(properties);
    return properties;
  }

  public Configurations getConfigurations() {
    Preconditions.checkNotNull(configurations);
    return configurations;
  }

  public SQLParser getSqlParser() {
    Preconditions.checkNotNull(sqlParser);
    return sqlParser;
  }

  public ClusterState getClusterState() {
    Preconditions.checkNotNull(clusterState);
    return clusterState;
  }

  public TransportService getTransportService() {
    Preconditions.checkNotNull(transportService);
    return transportService;
  }

  public PersistRepository getPersistRepository() {
    Preconditions.checkNotNull(persistRepository);
    return persistRepository;
  }

  public IdGenerator getIdGenerator() {
    Preconditions.checkNotNull(idGenerator);
    return idGenerator;
  }

  public ThreadPool getThreadPool() {
    Preconditions.checkNotNull(threadPool);
    return threadPool;
  }

  public static class Builder {
    private static final String PROPERTIES_FILE_NAME = "myproxy.properties";

    private final Properties properties;
    private final Configurations configurations;

    public Builder() {
      this.properties = loadConfigurationProperties();
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

    private SQLParser newSQLParser() {
      return new SQLParser(true);
    }

    public ServerContext build() {
      return new ServerContext(properties, configurations, newSQLParser());
    }
  }
}
