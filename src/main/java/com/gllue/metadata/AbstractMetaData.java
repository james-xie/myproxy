package com.gllue.metadata;

import com.gllue.common.io.stream.StreamInput;
import com.gllue.common.io.stream.StreamOutput;
import com.google.common.base.Preconditions;
import java.lang.ref.WeakReference;
import lombok.Getter;
import lombok.Setter;

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

  protected void doWrite(StreamOutput output) {
    output.writeStringNul(identity);
    output.writeInt(version);
  }

  @Override
  public void writeTo(StreamOutput output) {
    doWrite(output);
  }
}
