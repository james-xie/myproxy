package com.gllue.myproxy.metadata;

public abstract class AbstractMetaDataBuilder<M> implements MetaDataBuilder<M> {
  protected String identity;
  protected int version = MetaData.DEFAULT_VERSION;

  @Override
  public void copyFrom(M metadata, CopyOptions options) {
    if (metadata instanceof AbstractMetaData) {
      var absMetaData = (AbstractMetaData) metadata;
      this.identity = absMetaData.identity;
      this.version = absMetaData.version;
    } else {
      throw new IllegalArgumentException(
          String.format("Unknown type [%s]", metadata.getClass().getName()));
    }
  }
}
