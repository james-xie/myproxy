package com.gllue.myproxy.metadata.codec;

import com.gllue.myproxy.common.io.stream.StreamInput;
import com.gllue.myproxy.common.io.stream.StreamOutput;
import com.gllue.myproxy.metadata.MetaData;

public interface MetaDataCodec<T extends MetaData> {
  void encode(StreamOutput stream, T metaData);

  T decode(StreamInput stream);
}
