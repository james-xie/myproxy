package com.gllue.myproxy.common.properties;

import com.gllue.myproxy.common.util.ReflectionUtils;
import com.google.common.base.Preconditions;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/** Typed property value. */
@Slf4j
@Getter
public final class TypedPropertyValue {

  public enum Type {
    STRING("String", StringPropertyValueConvertor.class),
    BOOLEAN("Boolean", BooleanPropertyValueConvertor.class),
    INTEGER("Integer", IntegerPropertyValueConvertor.class),
    LONG("Long", LongPropertyValueConvertor.class),
    DOUBLE("Double", DoublePropertyValueConvertor.class),
    LIST_OF_INTEGER("ListOfInteger", ListOfIntegerPropertyValueConvertor.class),
    LIST_OF_STRING("ListOfString", ListOfStringPropertyValueConvertor.class);

    @Getter private final String name;
    @Getter private final TypedPropertyValueConvertor<?> convertor;

    private Type(final String name, final Class<? extends TypedPropertyValueConvertor<?>> clazz) {
      this.name = name;
      try {
        convertor = ReflectionUtils.newInstanceWithNoArgsConstructor(clazz);
      } catch (NoSuchMethodException
          | IllegalAccessException
          | InvocationTargetException
          | InstantiationException e) {
        log.error(
            "Failed to create instance for TypedPropertyValueConvertor[{}]",
            clazz.getSimpleName(),
            e);
        throw new TypedPropertyValueConvertorInstantiationException(clazz);
      }
    }

    public Object convert(final String value) {
      Preconditions.checkNotNull(value);
      return convertor.convert(value);
    }
  }

  private final Object value;

  public TypedPropertyValue(final TypedPropertyKey key, final String value)
      throws TypedPropertyValueException {
    this.value = key.getType().convert(value);
  }

  private static class StringPropertyValueConvertor implements TypedPropertyValueConvertor<String> {
    @Override
    public String convert(String value) {
      return value;
    }
  }

  private static class BooleanPropertyValueConvertor
      implements TypedPropertyValueConvertor<Boolean> {
    @Override
    public Boolean convert(String value) {
      if ("true".equalsIgnoreCase(value)) {
        return true;
      } else if ("false".equalsIgnoreCase(value)) {
        return false;
      }
      throw new TypedPropertyValueException(value, Boolean.class);
    }
  }

  private static class IntegerPropertyValueConvertor
      implements TypedPropertyValueConvertor<Integer> {
    @Override
    public Integer convert(String value) {
      try {
        return Integer.valueOf(value);
      } catch (NumberFormatException e) {
        throw new TypedPropertyValueException(value, Integer.class);
      }
    }
  }

  private static class LongPropertyValueConvertor implements TypedPropertyValueConvertor<Long> {
    @Override
    public Long convert(String value) {
      try {
        return Long.valueOf(value);
      } catch (NumberFormatException e) {
        throw new TypedPropertyValueException(value, Long.class);
      }
    }
  }

  private static class DoublePropertyValueConvertor implements TypedPropertyValueConvertor<Double> {
    @Override
    public Double convert(String value) {
      try {
        return Double.valueOf(value);
      } catch (NumberFormatException e) {
        throw new TypedPropertyValueException(value, Double.class);
      }
    }
  }

  private static class ListOfIntegerPropertyValueConvertor
      implements TypedPropertyValueConvertor<List<Integer>> {
    private static final String SEPARATOR = ",";

    @Override
    public List<Integer> convert(String value) {
      try {
        return Arrays.stream(value.split(SEPARATOR))
            .map(String::trim)
            .filter((x) -> !x.isEmpty())
            .map(Integer::valueOf)
            .collect(Collectors.toList());
      } catch (NumberFormatException e) {
        throw new TypedPropertyValueException(value, "ListOfInteger");
      }
    }
  }

  private static class ListOfStringPropertyValueConvertor
      implements TypedPropertyValueConvertor<List<String>> {
    private static final char ESCAPE_CHAR = '\\';
    private static final char SEPARATOR = ',';

    @Override
    public List<String> convert(String value) {
      char[] chars = value.toCharArray();
      List<String> items = new ArrayList<>();

      int index = 0;
      int maxIndex = chars.length - 1;
      StringBuilder buffer = new StringBuilder();
      while (index <= maxIndex) {
        if (chars[index] == SEPARATOR) {
          var item = buffer.toString().trim();
          if (!item.isEmpty()) {
            items.add(item);
          }
          buffer = new StringBuilder();
        } else if (index < maxIndex
            && chars[index] == ESCAPE_CHAR
            && chars[index + 1] == SEPARATOR) {
          index++;
          buffer.append(SEPARATOR);
        } else {
          buffer.append(chars[index]);
        }

        index++;
      }

      var item = buffer.toString().trim();
      if (!item.isEmpty()) {
        items.add(item);
      }
      return items;
    }
  }
}
