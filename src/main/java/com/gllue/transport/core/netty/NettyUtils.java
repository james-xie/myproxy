package com.gllue.transport.core.netty;

import io.netty.channel.Channel;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class NettyUtils {
  public static void closeChannel(final Channel channel, final boolean blocking) {
    try {
      if (channel != null && channel.isActive() && channel.isOpen()) {
        var future = channel.close();
        future.addListener((f) -> {
          if (f.isSuccess()) {
            if (log.isDebugEnabled()) {
              log.debug("Channel was closed successfully. [{}]", channel.remoteAddress());
            }
          } else {
            log.error("Failed to close channel. [{}]", channel.remoteAddress());
          }
        });
        if (blocking) {
          future.awaitUninterruptibly();
        }
      }
    } catch (Exception e) {
      log.error("An exception was occurred when closing channel. [{}]", channel.remoteAddress());
    }
  }

  public static void closeChannel(final Channel channel, final Consumer<Channel> onClosed) {
    try {
      if (channel != null && channel.isActive() && channel.isOpen()) {
        var future = channel.close();
        future.addListener(
            (f) -> {
              if (f.isSuccess()) {
                if (log.isDebugEnabled()) {
                  log.debug("Channel was closed successfully. [{}]", channel.remoteAddress());
                }
                onClosed.accept(channel);
              } else {
                log.error("Failed to close channel. [{}]", channel.remoteAddress());
              }
            });
      }
    } catch (Exception e) {
      log.error("Failed to close channel. [{}]", channel.remoteAddress());
    }
  }
}
