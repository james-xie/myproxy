package com.gllue.myproxy.common.util;

import static org.junit.Assert.assertNotEquals;

import java.security.NoSuchAlgorithmException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SecurityUtilsTest {
  public String formatDigest(byte[] digest) {
    var sb = new StringBuilder();
    sb.append("length: ");
    sb.append(digest.length);
    sb.append("\ncontent: ");
    for (int i = 0; i < digest.length - 1; i++) {
      sb.append(digest[i] & 0xff);
      sb.append(",");
    }
    if (digest.length > 0) {
      sb.append(digest[digest.length - 1]);
    }
    return sb.toString();
  }

  @Test
  public void testSha1Digest() throws NoSuchAlgorithmException {
    var input = new byte[100];
    var output = SecurityUtils.sha1Digest(input);
    assertNotEquals(input, output);

    //    var d1 = SecurityUtils.sha1Digest("james".getBytes());
    //    System.out.println(formatDigest(d1));
  }

  @Test
  public void testSha256Digest() throws NoSuchAlgorithmException {
    var input = new byte[100];
    var output = SecurityUtils.sha256Digest(input);
    assertNotEquals(input, output);
  }

  @Test
  public void testMD5Digest() throws NoSuchAlgorithmException {
    var input = new byte[100];
    var output = SecurityUtils.md5Digest(input);
    assertNotEquals(input, output);
  }

}
