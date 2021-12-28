package com.gllue.myproxy.common.properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.GenericConfigProperties;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import com.gllue.myproxy.config.TransportConfigProperties;
import java.util.List;
import java.util.Properties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

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
