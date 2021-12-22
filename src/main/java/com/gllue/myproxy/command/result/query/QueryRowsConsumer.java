package com.gllue.myproxy.command.result.query;

import com.gllue.myproxy.command.result.CommandResult;
import com.gllue.myproxy.common.Callback;
import java.util.function.Consumer;

public interface QueryRowsConsumer extends Callback<CommandResult>, Consumer<Row> {

  /**
   * Begin to read query rows. This method is invoked before the accept() method and is only called
   * once.
   *
   * @param metaData column definitions of the query result.
   */
  void begin(QueryResultMetaData metaData);

  /**
   * End of read query rows. This method is invoked after the accept() method and is only called
   * once.
   */
  void end();
}
