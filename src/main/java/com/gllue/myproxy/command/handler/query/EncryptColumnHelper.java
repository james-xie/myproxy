package com.gllue.myproxy.command.handler.query;

import com.gllue.myproxy.sql.parser.SQLCommentAttributeKey;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EncryptColumnHelper {
  public static String getEncryptKey(final QueryHandlerRequest request) {
    String encryptKey =
        (String) request.getCommentsAttributes().get(SQLCommentAttributeKey.ENCRYPT_KEY);
    if (encryptKey == null) {
      encryptKey = request.getSessionContext().getEncryptKey();
    }
    return encryptKey;
  }
}
