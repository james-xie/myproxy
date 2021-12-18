package com.gllue.myproxy.transport.backend.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.transport.backend.BackendResultReadException;
import com.gllue.myproxy.transport.exception.MySQLServerErrorCode;
import com.gllue.myproxy.transport.protocol.packet.generic.EofPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.ErrPacket;
import com.gllue.myproxy.transport.protocol.packet.generic.OKPacket;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultCommandResultReaderTest extends BaseCommandResultReaderTest {
  CommandResultReader prepareReader() {
    return super.prepareReader(new DefaultCommandResultReader());
  }

  @Test
  public void testReadOk() {
    var reader = prepareReader();
    var okPacket = new OKPacket(10, 11);
    var executionState = newExecutionStateRef();
    reader.addCallback(
        new Callback<>() {
          @Override
          public void onSuccess(CommandResult result) {
            executionState.set(CallbackExecutionState.EXECUTION_SUCCEED);
            assertEquals(okPacket.getAffectedRows(), result.getAffectedRows());
            assertEquals(okPacket.getLastInsertId(), result.getLastInsertId());
          }

          @Override
          public void onFailure(Throwable e) {
            executionState.set(CallbackExecutionState.EXECUTION_FAILED);
          }
        });
    var readCompleted = reader.read(packetToPayload(okPacket));
    reader.fireReadCompletedEvent();

    assertTrue(readCompleted);
    assertEquals(CallbackExecutionState.EXECUTION_SUCCEED, executionState.get());
  }

  @Test
  public void testReadErr() {
    var reader = prepareReader();
    var errPacket = new ErrPacket(MySQLServerErrorCode.ER_NET_READ_ERROR);
    var executionState = newExecutionStateRef();
    reader.addCallback(
        new Callback<>() {
          @Override
          public void onSuccess(CommandResult result) {
            executionState.set(CallbackExecutionState.EXECUTION_SUCCEED);
          }

          @Override
          public void onFailure(Throwable e) {
            executionState.set(CallbackExecutionState.EXECUTION_FAILED);
            Assert.assertThat(e, IsInstanceOf.instanceOf(BackendResultReadException.class));
            var exc = (BackendResultReadException) e;
            assertEquals(errPacket.getErrorCode(), exc.getErrorCode().getErrorCode());
          }
        });
    var readCompleted = reader.read(packetToPayload(errPacket));
    reader.fireReadCompletedEvent();

    assertTrue(readCompleted);
    assertEquals(CallbackExecutionState.EXECUTION_FAILED, executionState.get());
  }

  @Test
  public void readReadEof() {
    var reader = prepareReader();
    var executionState = newExecutionStateRef();
    reader.addCallback(
        new Callback<>() {
          @Override
          public void onSuccess(CommandResult result) {
            executionState.set(CallbackExecutionState.EXECUTION_SUCCEED);
          }

          @Override
          public void onFailure(Throwable e) {
            executionState.set(CallbackExecutionState.EXECUTION_FAILED);
          }
        });
    var readCompleted = reader.read(packetToPayload(new EofPacket()));
    reader.fireReadCompletedEvent();

    assertTrue(readCompleted);
    assertEquals(CallbackExecutionState.EXECUTION_FAILED, executionState.get());
  }
}
