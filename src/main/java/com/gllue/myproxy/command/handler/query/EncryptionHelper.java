package com.gllue.myproxy.command.handler.query;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.gllue.myproxy.sql.parser.SQLCommentAttributeKey;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EncryptionHelper {
  public enum EncryptionAlgorithm {
    AES;

    public static EncryptionAlgorithm getAlgorithmByName(final String name) {
      for (var value : EncryptionAlgorithm.values()) {
        if (value.name().equalsIgnoreCase(name)) {
          return value;
        }
      }
      throw new UnknownEncryptionAlgorithmException(name);
    }
  }

  @RequiredArgsConstructor
  public static final class AesEncryptor implements Encryptor {
    private final String key;

    /** Wrap the expression with AES_ENCRYPT() function. */
    @Override
    public String encryptExpr(String expr) {
      if (key == null) {
        throw new NoEncryptKeyException();
      }
      return String.format("AES_ENCRYPT(%s, '%s')", expr, key);
    }
  }

  @RequiredArgsConstructor
  public static final class AesDecryptor implements Decryptor {
    private static final String CHARSET = "utf8mb4";
    private final String key;

    /** Wrap the expression with AES_DECRYPT() function. */
    @Override
    public String decryptExpr(String expr) {
      if (key == null) {
        throw new NoEncryptKeyException();
      }
      return String.format("CONVERT(AES_DECRYPT(%s, '%s') USING '%s')", expr, key, CHARSET);
    }
  }

  public static Encryptor newEncryptor(EncryptionAlgorithm algorithm, final String key) {
    if (algorithm == EncryptionAlgorithm.AES) {
      return new AesEncryptor(key);
    }
    throw new UnknownEncryptionAlgorithmException(algorithm.name());
  }

  public static Decryptor newDecryptor(EncryptionAlgorithm algorithm, final String key) {
    if (algorithm == EncryptionAlgorithm.AES) {
      return new AesDecryptor(key);
    }
    throw new UnknownEncryptionAlgorithmException(algorithm.name());
  }

  public static String getEncryptKey(final QueryHandlerRequest request) {
    String encryptKey =
        (String) request.getCommentsAttributes().get(SQLCommentAttributeKey.ENCRYPT_KEY);
    if (encryptKey == null) {
      encryptKey = request.getSessionContext().getEncryptKey();
    }
    return encryptKey;
  }

  public static boolean isValidOperator(SQLBinaryOperator operator) {
    switch (operator) {
      case Equality:
      case NotEqual:
      case Is:
      case IsNot:
        return true;
    }
    return false;
  }
}
