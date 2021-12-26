package com.gllue.myproxy.command.handler;

import com.gllue.myproxy.common.Callback;

public interface CommandHandler<Request extends HandlerRequest> {
  /**
   *
   *
   * <pre>
   * The name of the handler.
   * Note: The name must be unique across handlers.
   * </pre>
   */
  String name();

  /**
   * Execute the command handler.
   *
   * @param request handler request
   * @param callback execution callback
   */
  void execute(Request request, Callback<HandlerResult> callback);
}
