package com.gllue.myproxy;

import com.gllue.myproxy.common.io.stream.ByteArrayStreamInput;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TestHelper {
  public static TableMetaData bytesToTableMetaData(byte[] bytes) {
    var input = new ByteArrayStreamInput(bytes);
    var builder = new TableMetaData.Builder();
    builder.readStream(input);
    return builder.build();
  }

  public static PartitionTableMetaData bytesToPartitionTableMetaData(byte[] bytes) {
    var input = new ByteArrayStreamInput(bytes);
    var builder = new PartitionTableMetaData.Builder();
    builder.readStream(input);
    return builder.build();
  }
}
