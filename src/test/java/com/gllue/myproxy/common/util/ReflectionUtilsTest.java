package com.gllue.myproxy.common.util;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ReflectionUtilsTest {
  private static class ConstructorAutoGeneratedTestClass {}

  static class PrivateConstructorTestClass {
    private PrivateConstructorTestClass() {}
  }

  static class PublicConstructorTestClass {
    public PublicConstructorTestClass() {}
  }

  @Test
  public void testNewInstanceWithNoArgsConstructor() throws Exception {
    var ins1 =
        ReflectionUtils.newInstanceWithNoArgsConstructor(ConstructorAutoGeneratedTestClass.class);
    assertNotNull(ins1);
    assertThat(ins1, instanceOf(ConstructorAutoGeneratedTestClass.class));

    var ins2 = ReflectionUtils.newInstanceWithNoArgsConstructor(PrivateConstructorTestClass.class);
    assertNotNull(ins2);
    assertThat(ins2, instanceOf(PrivateConstructorTestClass.class));

    var ins3 = ReflectionUtils.newInstanceWithNoArgsConstructor(PublicConstructorTestClass.class);
    assertNotNull(ins3);
    assertThat(ins3, instanceOf(PublicConstructorTestClass.class));
  }
}
