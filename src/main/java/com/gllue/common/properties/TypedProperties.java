package com.gllue.common.properties;

import com.gllue.config.ConfigurationException;
import com.gllue.config.Configurations;
import com.google.common.base.Joiner;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import lombok.Getter;

/** Typed properties with a specified enum. */
public abstract class TypedProperties<E extends Enum<?> & TypedPropertyKey> {

  @Getter private final Properties props;

  private final Map<E, TypedPropertyValue> cache;

  protected TypedProperties(final Class<E> keyClass, final Properties props) {
    this.props = props;
    cache = preload(keyClass);
  }

  private Map<E, TypedPropertyValue> preload(final Class<E> keyClass) {
    var enumConstants = keyClass.getEnumConstants();
    var result = new HashMap<E, TypedPropertyValue>(enumConstants.length, 1);
    var errorMessages = new LinkedList<>();
    for (E each : enumConstants) {
      TypedPropertyValue value;
      var key = each.getCompleteKey();
      try {
        var valueStr = props.getOrDefault(key, each.getDefaultValue()).toString();
        valueStr = valueStr.trim();
        value = new TypedPropertyValue(each, valueStr);
      } catch (final TypedPropertyValueException ex) {
        errorMessages.add(String.format("Property [%s]: %s", key, ex.getMessage()));
        continue;
      }
      result.put(each, value);
    }
    if (!errorMessages.isEmpty()) {
      throw new ConfigurationException(Joiner.on(System.lineSeparator()).join(errorMessages));
    }
    return result;
  }

  /**
   * Get property value.
   *
   * @param key property key
   * @param <T> class type of return value
   * @return property value
   */
  @SuppressWarnings("unchecked")
  public final <T> T getValue(final E key) {
    return (T) cache.get(key).getValue();
  }

  public abstract Configurations.Type getType();
}
