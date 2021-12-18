package com.gllue.myproxy.common.util;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NetworkUtils {
  /** Return all interfaces (and subinterfaces) on the system */
  static List<NetworkInterface> getInterfaces() throws SocketException {
    List<NetworkInterface> all =
        new ArrayList<>(Collections.list(NetworkInterface.getNetworkInterfaces()));
    all.sort(Comparator.comparingInt(NetworkInterface::getIndex));
    return all;
  }

  /** Returns all interface-local scope (loopback) addresses for interfaces that are up. */
  static InetAddress[] getLoopbackAddresses() throws SocketException {
    List<InetAddress> list = new ArrayList<>();
    for (NetworkInterface intf : getInterfaces()) {
      if (intf.isUp()) {
        for (InetAddress address : Collections.list(intf.getInetAddresses())) {
          if (address.isLoopbackAddress()) {
            list.add(address);
          }
        }
      }
    }
    if (list.isEmpty()) {
      throw new IllegalArgumentException(
          "No up-and-running loopback addresses found, got " + getInterfaces());
    }
    return list.toArray(new InetAddress[0]);
  }

  public static InetAddress[] resolveNetworkAddress(final String host) throws IOException {
    return InetAddress.getAllByName(host);
  }

  public static SocketAddress[] resolveSocketAddress(final String host, final int port)
      throws IOException {
    var inetAddrArray = resolveNetworkAddress(host);
    var socketAddress = new SocketAddress[inetAddrArray.length];
    for (int i=0; i<inetAddrArray.length; i++) {
      socketAddress[i] = new InetSocketAddress(inetAddrArray[i], port);
    }
    return socketAddress;
  }
}
