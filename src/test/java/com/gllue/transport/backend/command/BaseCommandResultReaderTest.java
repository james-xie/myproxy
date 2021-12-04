package com.gllue.transport.backend.command;

import com.gllue.transport.BaseTransportTest;
import com.gllue.transport.backend.connection.BackendConnection;
import com.gllue.transport.backend.connection.BackendConnectionImpl;
import com.gllue.transport.frontend.connection.FrontendConnection;
import com.gllue.transport.frontend.connection.FrontendConnectionImpl;
import io.netty.channel.embedded.EmbeddedChannel;
import java.util.concurrent.atomic.AtomicReference;

public class BaseCommandResultReaderTest extends BaseTransportTest {
  enum CallbackExecutionState {
    EXECUTION_SUCCEED,
    EXECUTION_FAILED,
    NOT_DONE
  }

  EmbeddedChannel backendChannel = new EmbeddedChannel();
  BackendConnection backendConnection = new BackendConnectionImpl(2, backendChannel);

  EmbeddedChannel frontendChannel = new EmbeddedChannel();
  FrontendConnection frontendConnection = new FrontendConnectionImpl(1, frontendChannel, "datasource");

  CommandResultReader prepareReader(CommandResultReader reader) {
    reader.bindConnection(backendConnection);
    return reader;
  }

  AtomicReference<CallbackExecutionState> newExecutionStateRef() {
    return new AtomicReference<>(CallbackExecutionState.NOT_DONE);
  }

}
