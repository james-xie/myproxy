package com.gllue.myproxy.metadata.codec;

import com.gllue.myproxy.common.exception.BaseServerException;

public class MetaDataCodecException extends BaseServerException {
  public MetaDataCodecException(String msg, Object... args) {
    super(msg, args);
  }
}
