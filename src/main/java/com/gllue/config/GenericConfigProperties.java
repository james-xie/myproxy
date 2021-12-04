package com.gllue.config;

import com.gllue.common.properties.TypedProperties;
import com.gllue.common.properties.TypedPropertyValue.Type;
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
