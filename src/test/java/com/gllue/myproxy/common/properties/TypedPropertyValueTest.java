package com.gllue.myproxy.common.properties;

import static org.junit.Assert.assertEquals;

import com.gllue.myproxy.common.properties.TypedPropertyValue.Type;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TypedPropertyValueTest {
  @Test
  public void testStringValue() {
    assertEquals("abc", Type.STRING.convert("abc"));
  }

  @Test
  public void testBooleanValue() {
    assertEquals(true, Type.BOOLEAN.convert("true"));
    assertEquals(false, Type.BOOLEAN.convert("false"));
    assertEquals(true, Type.BOOLEAN.convert("TRUE"));
    assertEquals(true, Type.BOOLEAN.convert("True"));
    assertEquals(false, Type.BOOLEAN.convert("False"));
    assertEquals(false, Type.BOOLEAN.convert("FALSE"));
  }

  @Test(expected = TypedPropertyValueException.class)
  public void testBadBooleanValue() {
    Type.BOOLEAN.convert("abc");
  }

  @Test
  public void testIntegerValue() {
    assertEquals(0, Type.INTEGER.convert("0"));
    assertEquals(-10, Type.INTEGER.convert("-10"));
    assertEquals(10, Type.INTEGER.convert("10"));
    assertEquals(10000, Type.INTEGER.convert("10000"));
    assertEquals(999999, Type.INTEGER.convert("999999"));
  }

  @Test(expected = TypedPropertyValueException.class)
  public void testBadIntegerValue() {
    Type.INTEGER.convert("100.1");
  }

  @Test
  public void testLongValue() {
    assertEquals(10L, Type.LONG.convert("10"));
    assertEquals(10000L, Type.LONG.convert("10000"));
    assertEquals(999999L, Type.LONG.convert("999999"));
    assertEquals(-999999L, Type.LONG.convert("-999999"));
  }

  @Test(expected = TypedPropertyValueException.class)
  public void testBadLongValue() {
    Type.LONG.convert("abd100");
  }

  @Test
  public void testDoubleValue() {
    assertEquals(100.999D, Type.DOUBLE.convert("100.999"));
    assertEquals(-9.99D, Type.DOUBLE.convert("-9.99"));
    assertEquals(.99D, Type.DOUBLE.convert(".99"));
    assertEquals(1.99D, Type.DOUBLE.convert("01.99"));
    assertEquals(1.99D, Type.DOUBLE.convert("1.99D"));
  }

  @Test(expected = TypedPropertyValueException.class)
  public void testBadDoubleValue() {
    Type.DOUBLE.convert("09.99DFE");
  }

  @Test
  public void testListOfIntegerValue() {
    assertEquals(List.of(9, 99, 999, 9999), Type.LIST_OF_INTEGER.convert("9,99,999,9999"));
    assertEquals(List.of(9, 99, 999, 9999), Type.LIST_OF_INTEGER.convert(",9,99,,,999,9999,"));
    assertEquals(List.of(1, 1, 2, 2, 3, 4), Type.LIST_OF_INTEGER.convert("1,1,2,2,3,4"));
  }

  @Test(expected = TypedPropertyValueException.class)
  public void testBadListOfIntegerValue() {
    Type.LIST_OF_INTEGER.convert("0,1,2,a,b,c");
  }

  @Test
  public void testListOfStringValue() {
    assertEquals(List.of("9", "99", "999", "9999"), Type.LIST_OF_STRING.convert("9,99,999,9999"));
    assertEquals(List.of("a", "bc", "def", "ghi"), Type.LIST_OF_STRING.convert("a,bc,def,ghi"));
  }
}
