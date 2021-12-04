package com.gllue.transport.backend.datasource;

import com.gllue.common.util.NetworkUtils;
import com.gllue.config.ConfigurationException;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class DataSourceConfig {
  private final String name;
  private final SocketAddress address;
  private final String user;
  private final String password;
  private final String database;

  public static class Parser {
    private static final String KV_SEPARATOR = "=";
    private static final String KV_PAIR_SEPARATOR = ";";
    private static final String[] REQUIRED_KEYS =
        new String[] {"name", "host", "user", "password", "database"};

    private Map<String, String> parseKeyValuePairs(final String config) {
      Map<String, String> kvs = new HashMap<>();
      for (var kv : config.split(KV_PAIR_SEPARATOR)) {
        var items = kv.split(KV_SEPARATOR, 2);
        if (items.length != 2) {
          throw new ConfigurationException(
              String.format("Invalid data source configuration. [%s]", config));
        }
        var key = items[0].trim();
        var old = kvs.putIfAbsent(key, items[1].trim());
        if (old != null) {
          throw new ConfigurationException(String.format("Duplicate configuration key. [%s]", key));
        }
      }
      return kvs;
    }

    private int getPort(Map<String, String> kvs) {
      var port = kvs.getOrDefault("port", "3306");
      try {
        return Integer.parseInt(port);
      } catch (NumberFormatException e) {
        throw new ConfigurationException(String.format("Port is not a number. [%s]", port));
      }
    }

    private void ensureRequiredKeys(Map<String, String> kvs) {
      for (var key : REQUIRED_KEYS) {
        var value = kvs.get(key);
        if (value == null) {
          throw new ConfigurationException(String.format("Key [%s] is required.", key));
        }
        if (value.isBlank()) {
          throw new ConfigurationException(String.format("Value for key [%s] cannot empty.", key));
        }
      }
    }

    private SocketAddress resolveSocketAddress(Map<String, String> kvs) {
      SocketAddress socketAddress;
      try {
        var socketAddresses = NetworkUtils.resolveSocketAddress(kvs.get("host"), getPort(kvs));
        if (socketAddresses.length == 0) {
          throw new ConfigurationException(
              String.format("Unable to resolve host. [%s]", kvs.get("host")));
        }
        socketAddress = socketAddresses[0];
      } catch (IOException e) {
        throw new ConfigurationException(String.format("Bad host. [%s]", kvs.get("host")), e);
      }
      return socketAddress;
    }

    public DataSourceConfig parse(final String config) {
      var kvs = parseKeyValuePairs(config);
      ensureRequiredKeys(kvs);
      return new DataSourceConfig(
          kvs.get("name"),
          resolveSocketAddress(kvs),
          kvs.get("user"),
          kvs.get("password"),
          kvs.get("database"));
    }
  }
}
