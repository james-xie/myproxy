package com.gllue.myproxy.common.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ReflectionUtils {
  public static <T> T newInstanceWithNoArgsConstructor(Class<T> clazz)
      throws NoSuchMethodException, SecurityException, IllegalAccessException,
          InvocationTargetException, InstantiationException {
    var parameterTypes = new Class<?>[0];
    var constructor = clazz.getDeclaredConstructor(parameterTypes);
    makeAccessible(constructor);
    return constructor.newInstance();
  }

  /**
   * Make the given constructor accessible, explicitly setting it accessible
   * if necessary. The {@code setAccessible(true)} method is only called
   * when actually necessary, to avoid unnecessary conflicts with a JVM
   * SecurityManager (if active).
   * @param ctor the constructor to make accessible
   * @see java.lang.reflect.Constructor#setAccessible
   */
  public static void makeAccessible(Constructor<?> ctor) {
    if (!Modifier.isPublic(ctor.getModifiers()) ||
        !Modifier.isPublic(ctor.getDeclaringClass().getModifiers())) {
      ctor.setAccessible(true);
    }
  }
}
