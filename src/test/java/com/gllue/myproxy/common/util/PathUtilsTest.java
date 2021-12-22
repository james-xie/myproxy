package com.gllue.myproxy.common.util;

import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.config.Configurations.Type;
import com.gllue.myproxy.config.GenericConfigPropertyKey;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PathUtilsTest {
  @Mock Configurations configurations;

  @Test
  public void testGetRootPath() {
    Mockito.when(
            configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.REPOSITORY_ROOT_PATH))
        .thenReturn("/root_path");
    Assert.assertEquals("/root_path", PathUtils.getRootPath(configurations));

    Mockito.when(
            configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.REPOSITORY_ROOT_PATH))
        .thenReturn("/root_path/");
    Assert.assertEquals("/root_path", PathUtils.getRootPath(configurations));

    Mockito.when(
            configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.REPOSITORY_ROOT_PATH))
        .thenReturn("root_path/");
    Assert.assertEquals("/root_path", PathUtils.getRootPath(configurations));

    Mockito.when(
            configurations.getValue(Type.GENERIC, GenericConfigPropertyKey.REPOSITORY_ROOT_PATH))
        .thenReturn("/根路径");
    Assert.assertEquals("/根路径", PathUtils.getRootPath(configurations));
  }

  @Test
  public void testJoinPaths() {
    Assert.assertEquals("/根路径/节点1/节点2", PathUtils.joinPaths("/根路径", "节点1", "节点2"));
  }
}
