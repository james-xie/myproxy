package com.gllue.config;

import com.gllue.common.properties.TypedProperties;
import java.util.Properties;

/** Typed properties for transport configuration. */
public final class TransportConfigProperties extends TypedProperties<TransportConfigPropertyKey> {
  public TransportConfigProperties(final Properties props) {
    super(TransportConfigPropertyKey.class, props);
  }

  @Override
  public Configurations.Type getType() {
    return Configurations.Type.TRANSPORT;
  }
}
