package com.gllue.config;

import com.gllue.common.properties.TypedProperties;
import com.gllue.common.properties.TypedPropertyKey;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Configurations {
  public enum Type {
    GENERIC,
    TRANSPORT;
  }

  private final Map<Type, ? extends TypedProperties> propsMap;

  @SuppressWarnings("unchecked")
  public Configurations(List<TypedProperties> props) {
    propsMap =
        Collections.unmodifiableMap(
            props.stream().collect(Collectors.toMap(TypedProperties::getType, (x) -> x)));
    for (var type : Type.values()) {
      if (!propsMap.containsKey(type)) {
        throw new IllegalArgumentException(String.format("Missing [%s] properties.", type.name()));
      }
    }
  }

  private TypedProperties getTypedProperties(final Type type) {
    return propsMap.get(type);
  }

  @SuppressWarnings("unchecked")
  public <T, E extends Enum<?> & TypedPropertyKey> T getValue(Type type, E key) {
    return (T) getTypedProperties(type).getValue(key);
  }
}
