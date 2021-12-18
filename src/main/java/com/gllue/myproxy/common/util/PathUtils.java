package com.gllue.myproxy.common.util;

import static org.apache.curator.utils.PathUtils.validatePath;

import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import com.gllue.myproxy.constant.ServerConstants;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.NONE)
public class PathUtils {
  public static String getRootPath(final Configurations configurations) {
    String rootPath =
        configurations.getValue(Configurations.Type.GENERIC, GenericConfigPropertyKey.REPOSITORY_ROOT_PATH);
    if (!rootPath.startsWith(ServerConstants.PATH_SEPARATOR)) {
      rootPath = ServerConstants.PATH_SEPARATOR + rootPath;
    }
    if (rootPath.endsWith(ServerConstants.PATH_SEPARATOR)) {
      rootPath = rootPath.substring(0, rootPath.length() - 1);
    }
    return validatePath(rootPath);
  }

  public static String joinPaths(final String... path) {
    return String.join(ServerConstants.PATH_SEPARATOR, path);
  }
}
