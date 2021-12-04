package com.gllue.common.properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.gllue.config.Configurations;
import com.gllue.config.Configurations.Type;
import com.gllue.config.GenericConfigProperties;
import com.gllue.config.GenericConfigPropertyKey;
import com.gllue.config.TransportConfigProperties;
import java.util.List;
import java.util.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConfigurationsTest {
  @Test
  public void testGetValue() {
    var properties = new Properties();
    var configurations =
        new Configurations(
            List.of(
                new GenericConfigProperties(properties),
                new TransportConfigProperties(properties)));
    int processors = configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.PROCESSORS);
    assertEquals(
        Integer.valueOf(GenericConfigPropertyKey.PROCESSORS.getDefaultValue()).intValue(),
        processors);
  }
}
