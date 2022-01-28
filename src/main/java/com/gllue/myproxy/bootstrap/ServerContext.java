package com.gllue.myproxy.bootstrap;

import static com.gllue.myproxy.constant.ServerConstants.DEFAULT_PROPERTIES_FILE_NAME;
import static com.gllue.myproxy.constant.ServerConstants.KEY_OF_PROPERTIES_LOCATION;

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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
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

    private InputStream getPropertiesResource(ClassLoader classLoader) {
      // First, read properties location from application arguments,
      // if not found, read properties location from environment variables.
      InputStream stream = null;
      var location = System.getProperty(KEY_OF_PROPERTIES_LOCATION);
      if (location == null) {
        location = System.getenv(KEY_OF_PROPERTIES_LOCATION);
      }

      if (location != null) {
        try {
          stream = new FileInputStream(location);
          log.info("Read properties file from location [{}].", location);
        } catch (FileNotFoundException e) {
          throw new BadPropertiesLocationException(location);
        }
      }
      if (stream == null) {
        stream = classLoader.getResourceAsStream(DEFAULT_PROPERTIES_FILE_NAME);
        log.info("Read properties file from default.");
      }
      return stream;
    }

    private Properties loadConfigurationProperties() {
      var properties = new Properties();
      var stream = getPropertiesResource(this.getClass().getClassLoader());
      if (stream == null) {
        throw new ConfigurationException(
            String.format("Properties file [%s] is not found.", DEFAULT_PROPERTIES_FILE_NAME));
      }

      try {
        try {
          properties.load(stream);
        } catch (IOException e) {
          throw new ConfigurationException(
              String.format(
                  "An exception is occurred when loading the properties file. [%s]",
                  DEFAULT_PROPERTIES_FILE_NAME));
        }
      } finally {
        try {
          stream.close();
        } catch (IOException e) {
          log.error("Failed to close properties stream.", e);
        }
      }

      if (log.isDebugEnabled()) {
        log.debug("Loaded properties: [{}]", properties);
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
