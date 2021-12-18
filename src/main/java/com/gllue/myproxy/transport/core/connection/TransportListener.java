package com.gllue.myproxy.transport.core.connection;

import com.gllue.myproxy.common.Callback;
import java.util.concurrent.Executor;

public interface TransportListener<T extends Connection> extends Callback<T> {
  Executor executor();
}
