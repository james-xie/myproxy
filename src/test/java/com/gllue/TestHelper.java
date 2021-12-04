package com.gllue;

import com.gllue.common.io.stream.ByteArrayStreamInput;
import com.gllue.metadata.model.PartitionTableMetaData;
import com.gllue.metadata.model.TableMetaData;
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
