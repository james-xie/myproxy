package com.gllue.command.handler;


import com.gllue.common.Callback;

public interface CommandHandler<Request extends HandlerRequest, Result extends HandlerResult> {
  /**
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
  void execute(Request request, Callback<Result> callback);
}
