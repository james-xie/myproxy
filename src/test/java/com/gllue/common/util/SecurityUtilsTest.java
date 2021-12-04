package com.gllue.common.util;

import static org.junit.Assert.assertNotEquals;

import com.gllue.transport.backend.netty.auth.CachingSha2PluginHandler;
import com.gllue.transport.constant.MySQLCapabilityFlag;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

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
