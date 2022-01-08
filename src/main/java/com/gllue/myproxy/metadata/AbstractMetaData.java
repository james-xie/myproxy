package com.gllue.myproxy.metadata;

import com.gllue.myproxy.common.io.stream.StreamInput;
import com.gllue.myproxy.common.io.stream.StreamOutput;
import com.google.common.base.Preconditions;
import lombok.Getter;

public abstract class AbstractMetaData implements MetaData {
  @Getter protected final String identity;
  @Getter protected int version;

  protected AbstractMetaData(final String identity, final int version) {
    Preconditions.checkNotNull(identity, "Meta data identity cannot be null.");
    this.identity = identity;
    this.version = version;
  }

  protected AbstractMetaData(final StreamInput input) {
    this(input.readStringNul(), input.readInt());
  }

}
