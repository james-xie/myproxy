package com.gllue.transport.core.connection;

import com.gllue.common.Callback;
import java.util.concurrent.Executor;

public interface TransportListener<T extends Connection> extends Callback<T> {
  Executor executor();
}
