package com.gllue.myproxy.config;

import com.gllue.myproxy.common.properties.TypedProperties;
import java.util.Properties;

/**
 * Typed properties for generic configuration.
 */
public final class GenericConfigProperties
    extends TypedProperties<GenericConfigPropertyKey> {
  public GenericConfigProperties(final Properties props) {
    super(GenericConfigPropertyKey.class, props);
  }

  @Override
  public Configurations.Type getType() {
    return Configurations.Type.GENERIC;
  }
}
