package com.gllue.myproxy.repository.zookeeper;

import com.gllue.myproxy.common.properties.TypedProperties;
import com.gllue.myproxy.config.Configurations;
import java.util.Properties;

/**
 * Typed properties for generic configuration.
 */
public final class ZookeeperConfigProperties
    extends TypedProperties<ZookeeperConfigPropertyKey> {
  public ZookeeperConfigProperties(final Properties props) {
    super(ZookeeperConfigPropertyKey.class, props);
  }

  @Override
  public Configurations.Type getType() {
    return Configurations.Type.GENERIC;
  }
}
