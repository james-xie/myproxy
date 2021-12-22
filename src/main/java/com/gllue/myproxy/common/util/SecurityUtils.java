package com.gllue.myproxy.common.util;

import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SecurityUtils {
public static final String RSA_SHA1_MGF1_PADDING_TRANSFORMATION = "RSA/ECB/OAEPWithSHA-1AndMGF1Padding";
  public static final String RAS_PKCS1_PADDING_TRANSFORMATION = "RSA/ECB/PKCS1Padding";

  public static MessageDigest sha1Instance() throws NoSuchAlgorithmException {
    return MessageDigest.getInstance("SHA-1");
  }

  public static MessageDigest md5Instance() throws NoSuchAlgorithmException {
    return MessageDigest.getInstance("MD5");
  }

  public static MessageDigest sha256Instance() throws NoSuchAlgorithmException {
    return MessageDigest.getInstance("SHA-256");
  }

  private static byte[] generateDigest(MessageDigest digest, final byte[]... inputs) {
    for (var input : inputs) {
      digest.update(input);
    }
    return digest.digest();
  }

  public static byte[] sha1Digest(final byte[]... inputs) throws NoSuchAlgorithmException {
    return generateDigest(SecurityUtils.sha1Instance(), inputs);
  }

  public static byte[] md5Digest(final byte[]... inputs) throws NoSuchAlgorithmException {
    return generateDigest(SecurityUtils.md5Instance(), inputs);
  }

  public static byte[] sha256Digest(final byte[]... inputs) throws NoSuchAlgorithmException {
    return generateDigest(SecurityUtils.sha256Instance(), inputs);
  }

  public static RSAPublicKey decodeRSAPublicKey(String key)
      throws NoSuchAlgorithmException, InvalidKeySpecException {
//    int offset = key.indexOf("\n") + 1;
//    int len = key.indexOf("-----END PUBLIC KEY-----") - offset;
//    byte[] keyBytes = new byte[len];
//    System.arraycopy(key.getBytes(), offset, keyBytes, 0, len);

    key = key.replace("-----BEGIN PUBLIC KEY-----", "");
    key = key.replace("-----END PUBLIC KEY-----", "");
    var keyBytes = key.replace("\n", "").getBytes();

    byte[] certificateData = Base64.getDecoder().decode(keyBytes);
    X509EncodedKeySpec spec = new X509EncodedKeySpec(certificateData);
    KeyFactory kf = KeyFactory.getInstance("RSA");
    return (RSAPublicKey) kf.generatePublic(spec);
  }

  public static byte[] encryptWithRSAPublicKey(
      byte[] source, RSAPublicKey key, String transformation)
      throws NoSuchPaddingException, NoSuchAlgorithmException, InvalidKeyException,
          BadPaddingException, IllegalBlockSizeException {
    Cipher cipher = Cipher.getInstance(transformation);
    cipher.init(Cipher.ENCRYPT_MODE, key);
    return cipher.doFinal(source);
  }
}
