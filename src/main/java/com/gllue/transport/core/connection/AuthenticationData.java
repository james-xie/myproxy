package com.gllue.transport.core.connection;

import lombok.Data;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AuthenticationData {
  private String username;
  private String hostname;
  private String password;
  private String databaseName;
  private byte[] authResponse;
  private String dataSource;

  public AuthenticationData(
      final String username,
      final String hostname,
      final String databaseName,
      final byte[] authResponse) {
    this(username, hostname, null, databaseName, authResponse, null);
  }

  public AuthenticationData(
      final String username, final String password, final String databaseName) {
    this(username, null, password, databaseName, null, null);
  }

}
