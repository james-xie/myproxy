package com.gllue.common.io.stream;

import com.google.common.base.Preconditions;

public abstract class AbstractStreamOutput implements StreamOutput {

  @Override
  public void writeBytes(byte[] buf) {
    writeBytes(buf, 0, buf.length);
  }

  @Override
  public void writeBoolean(boolean val) {
    writeByte((byte)(val ? 1 : 0));
  }

  @Override
  public void writeInt(int val) {
    writeByte((byte)((val >> 24) & 0xFF));
    writeByte((byte)((val >> 16) & 0xFF));
    writeByte((byte)((val >> 8) & 0xFF));
    writeByte((byte)(val & 0xFF));
  }

  @Override
  public void writeLong(long val) {
    writeInt((int)(val >> 32));
    writeInt((int)(val));
  }

  @Override
  public void writeFloat(float val) {
    writeInt(Float.floatToIntBits(val));
  }

  @Override
  public void writeDouble(double val) {
    writeLong(Double.doubleToLongBits(val));
  }

  @Override
  public void writeStringFix(String val) {
    Preconditions.checkNotNull(val);
    writeBytes(val.getBytes());
  }

  @Override
  public void writeStringNul(String val) {
    Preconditions.checkNotNull(val);
    writeBytes(val.getBytes());
    writeByte((byte)NULL);
  }

  @Override
  public void writeNullableString(String val) {
    if (val == null) {
      writeBoolean(true);
      return;
    }

    writeBoolean(false);
    writeStringNul(val);
  }

  @Override
  public void writeStringArray(String[] val) {
    Preconditions.checkNotNull(val);
    writeInt(val.length);
    for (var item: val) {
      writeStringNul(item);
    }
  }
}
