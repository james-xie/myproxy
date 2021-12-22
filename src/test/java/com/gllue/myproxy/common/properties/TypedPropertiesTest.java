package com.gllue.myproxy.common.properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.gllue.myproxy.common.properties.TypedPropertyValue.Type;
import com.gllue.myproxy.config.Configurations;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TypedPropertiesTest {

  static final String PROPERTIES_FILE_NAME = "typed_properties_test.properties";

  @Getter
  @RequiredArgsConstructor
  enum TestPropertyKey implements TypedPropertyKey {
    KEY_1("key_1", 100, Type.INTEGER),
    KEY_2("key_2", 100, Type.LONG),
    KEY_3("key_3", "stringValue", Type.STRING),
    KEY_4("key_4", true, Type.BOOLEAN),
    KEY_5("key_5", "1,2,3,4,5", Type.LIST_OF_INTEGER),
    KEY_6("key_6", "10,20,30", Type.LIST_OF_INTEGER),
    KEY_7("key_7", "", Type.LIST_OF_STRING);

    private static final String PREFIX = "test";

    private final String key;

    private final String defaultValue;

    private final Type type;

    TestPropertyKey(final String key, final Object defaultValue, final Type type) {
      this.key = key;
      this.defaultValue = String.valueOf(defaultValue);
      this.type = type;
    }

    @Override
    public String getPrefix() {
      return PREFIX;
    }
  }

  static class TestProperties extends TypedProperties<TestPropertyKey> {
    public TestProperties(final Properties properties) {
      super(TestPropertyKey.class, properties);
    }

    @Override
    public Configurations.Type getType() {
      return null;
    }
  }

  @Test
  public void testPreload() throws IOException {
    var props = new Properties();
    var stream = this.getClass().getClassLoader().getResourceAsStream(PROPERTIES_FILE_NAME);
    props.load(stream);
    var typedProps = new TestProperties(props);

    int value1 = typedProps.getValue(TestPropertyKey.KEY_1); // default value
    assertEquals(123, value1);

    long value2 = typedProps.getValue(TestPropertyKey.KEY_2);
    assertEquals(100L, value2);

    String value3 = typedProps.getValue(TestPropertyKey.KEY_3);
    assertEquals("abcdefg", value3);

    boolean value4 = typedProps.getValue(TestPropertyKey.KEY_4);
    assertFalse(value4);

    List<Integer> value5 = typedProps.getValue(TestPropertyKey.KEY_5); // default value
    assertEquals(List.of(1, 2, 3, 4, 5), value5);

    List<Integer> value6 = typedProps.getValue(TestPropertyKey.KEY_6); // default value
    assertEquals(List.of(), value6);

    List<String> value7 = typedProps.getValue(TestPropertyKey.KEY_7); // default value
    assertEquals(List.of("a=1,,,", "b=2", "c=3"), value7);
  }
}
