package com.gllue.myproxy;

import com.gllue.myproxy.common.io.stream.ByteArrayStreamInput;
import com.gllue.myproxy.metadata.codec.DatabaseMetaDataCodec;
import com.gllue.myproxy.metadata.codec.MetaDataCodec;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TestHelper {
  private static MetaDataCodec<DatabaseMetaData> getCodec() {
    return DatabaseMetaDataCodec.getInstance();
  }


  public static DatabaseMetaData bytesToMetaData(byte[] bytes) {
    var input = new ByteArrayStreamInput(bytes);
    return getCodec().decode(input);
  }

}
