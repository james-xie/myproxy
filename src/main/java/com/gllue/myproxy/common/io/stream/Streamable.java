package com.gllue.myproxy.common.io.stream;

/**
 * Implementing this interface is commonly for the purpose of serialize/deserialize an object
 * to/from a stream.
 */
public interface Streamable {
  /**
   * Set this object's fields from a {@linkplain StreamInput}.
   */
  void readFrom(StreamInput in);

  /**
   * Write this object's fields to a {@linkplain StreamOutput}.
   */
  void writeTo(StreamOutput out);
}
