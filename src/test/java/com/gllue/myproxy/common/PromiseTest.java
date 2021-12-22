package com.gllue.myproxy.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.common.Promise.Promises;
import com.gllue.myproxy.common.util.RandomUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PromiseTest {
  @Test
  public void testPromiseOnSuccess() {
    List<String> result = new ArrayList<>();
    new Promise<Boolean>(
            cb -> {
              result.add("resolve start");
              cb.onSuccess(true);
              result.add("resolve end");
            })
        .then(
            (value) -> {
              result.add("then 1 onSuccess: " + value);
              return 1;
            },
            (e) -> {
              result.add("then 1 onFailure: " + e);
              return 2;
            })
        .then(
            (value) -> {
              result.add("then 2 onSuccess: " + value);
              return true;
            },
            (e) -> {
              result.add("then 2 onFailure: " + e);
              return false;
            });
    assertEquals(
        List.of("resolve start", "resolve end", "then 1 onSuccess: true", "then 2 onSuccess: 1"),
        result);
  }

  @Test
  public void testPromiseOnFailure() {
    List<String> result = new ArrayList<>();
    new Promise<Boolean>(
            cb -> {
              result.add("resolve");
              throw new RuntimeException();
            })
        .then(
            (value) -> {
              result.add("then 1 onSuccess: " + value);
              throw new RuntimeException();
            },
            (e) -> {
              result.add("then 1 onFailure: " + e);
              return 2;
            })
        .then(
            (value) -> {
              result.add("then 2 onSuccess: " + value);
              return true;
            },
            (e) -> {
              result.add("then 2 onFailure: " + e);
              return false;
            });
    assertEquals(
        List.of("resolve", "then 1 onFailure: java.lang.RuntimeException", "then 2 onSuccess: 2"),
        result);
  }

  @Test
  public void testPromiseOnFinished() {
    List<String> result = new ArrayList<>();
    new Promise<Boolean>(
            cb -> {
              result.add("resolve");
              cb.onSuccess(true);
            })
        .doFinally(
            () -> {
              result.add("doFinally");
              return 1;
            })
        .then(
            (value) -> {
              result.add("then 1 onSuccess: " + value);
              return true;
            },
            (e) -> {
              result.add("then 1 onFailure: " + e);
              return false;
            })
        .doFinally(
            () -> {
              result.add("then 1 onFinished");
              return null;
            });

    assertEquals(
        List.of("resolve", "doFinally", "then 1 onSuccess: 1", "then 1 onFinished"), result);
  }

  @Test
  public void testPromiseOnFinishedWithArgs() {
    List<String> result = new ArrayList<>();
    new Promise<Boolean>(
            cb -> {
              result.add("resolve");
              cb.onSuccess(true);
            })
        .doFinally(
            (value, e) -> {
              result.add("doFinally: " + value);
              return value;
            })
        .then(
            (value) -> {
              result.add("then 1 onSuccess: " + value);
              throw new RuntimeException();
            },
            (e) -> {
              result.add("then 1 onFailure: " + e);
              return false;
            })
        .doFinally(
            (value, e) -> {
              result.add("doFinally: " + e);
              return value;
            });

    assertEquals(
        List.of(
            "resolve",
            "doFinally: true",
            "then 1 onSuccess: true",
            "doFinally: java.lang.RuntimeException"),
        result);
  }

  @Test
  public void testPromiseOnSuccessThenNull() {
    List<String> result = new ArrayList<>();
    new Promise<Boolean>(
            cb -> {
              result.add("resolve");
              cb.onSuccess(true);
            })
        .then(null)
        .then(
            (value) -> {
              result.add("then 1 onSuccess: " + value);
              return value;
            });

    assertEquals(List.of("resolve", "then 1 onSuccess: null"), result);
  }

  @Test
  public void testPromiseOnFailureThenNull() {
    List<String> result = new ArrayList<>();
    new Promise<Boolean>(
            cb -> {
              result.add("resolve");
              cb.onFailure(new RuntimeException());
            })
        .then(null)
        .doCatch(
            (e) -> {
              result.add("doCatch: " + e);
              return null;
            });

    assertEquals(List.of("resolve", "doCatch: java.lang.RuntimeException"), result);
  }

  @Test
  public void testPromiseOnSuccessAsync() throws InterruptedException {
    var latch = new CountDownLatch(1);
    var threadPool = Executors.newFixedThreadPool(1);
    Deque<String> result = new ConcurrentLinkedDeque<>();
    new Promise<Boolean>(
            cb -> {
              threadPool.execute(
                  () -> {
                    cb.onSuccess(true);
                  });
            })
        .then(
            (value) -> {
              result.add("then onSuccess: " + value);
              latch.countDown();
              return null;
            });

    latch.await();

    assertEquals(List.of("then onSuccess: true"), new ArrayList<>(result));
    threadPool.shutdown();
  }

  @Test
  public void testPromiseOnFailureAsync() throws InterruptedException {
    var latch = new CountDownLatch(1);
    var threadPool = Executors.newFixedThreadPool(1);
    Deque<String> result = new ConcurrentLinkedDeque<>();
    new Promise<Boolean>(
            cb -> {
              threadPool.execute(
                  () -> {
                    cb.onFailure(new RuntimeException());
                  });
            })
        .then(
            (value) -> {
              result.add("then onSuccess: " + value);
              return value;
            })
        .doCatch(
            (e) -> {
              result.add("doCatch 1: " + e);
              throw new RuntimeException();
            })
        .doCatch(
            (e) -> {
              result.add("doCatch 2: " + e);
              latch.countDown();
              return null;
            });

    latch.await();

    assertEquals(
        List.of("doCatch 1: java.lang.RuntimeException", "doCatch 2: java.lang.RuntimeException"),
        new ArrayList<>(result));
    threadPool.shutdown();
  }

  private void sleep(int timeInMills) {
    try {
      Thread.sleep(timeInMills);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testNestedPromiseOnSuccess() throws InterruptedException {
    var threadPool = Executors.newFixedThreadPool(1);
    var latch = new CountDownLatch(1);
    Deque<String> result = new ConcurrentLinkedDeque<>();
    new Promise<Boolean>(
            cb -> {
              threadPool.execute(
                  () -> {
                    sleep(10);
                    result.add("resolve");
                    cb.onSuccess(true);
                  });
            })
        .thenAsync(
            (value) -> {
              return new Promise<Integer>(
                      (cb) -> {
                        threadPool.execute(
                            () -> {
                              sleep(15);
                              result.add("thenAsync nested promise resolve: " + value);
                              cb.onSuccess(1);
                            });
                      })
                  .then(
                      (v) -> {
                        result.add("thenAsync nested promise then onSuccess: " + v);
                        return 2;
                      });
            })
        .then(
            (value) -> {
              result.add("then onSuccess: " + value);
              latch.countDown();
              return null;
            });

    latch.await();

    assertEquals(
        List.of(
            "resolve",
            "thenAsync nested promise resolve: true",
            "thenAsync nested promise then onSuccess: 1",
            "then onSuccess: 2"),
        new ArrayList<>(result));
    threadPool.shutdown();
  }

  @Test
  public void testNestedPromiseOnFailure() throws InterruptedException {
    var threadPool = Executors.newFixedThreadPool(1);
    var latch = new CountDownLatch(1);
    Deque<String> result = new ConcurrentLinkedDeque<>();
    new Promise<Boolean>(
            cb -> {
              threadPool.execute(
                  () -> {
                    sleep(10);
                    result.add("resolve");
                    cb.onFailure(new RuntimeException());
                  });
            })
        .doCatchAsync(
            (e) -> {
              return new Promise<Integer>(
                  (cb) -> {
                    threadPool.execute(
                        () -> {
                          sleep(10);
                          result.add("doCatchAsync nested promise resolve: " + e);
                          cb.onFailure(new RuntimeException());
                        });
                  });
            })
        .thenAsync(
            (value) -> {
              return new Promise<Boolean>(
                  (cb) -> {
                    threadPool.execute(
                        () -> {
                          sleep(5);
                          result.add("thenAsync nested promise resolve: " + value);
                          cb.onSuccess(true);
                        });
                  });
            },
            (e) -> {
              return new Promise<Boolean>(
                  (cb) -> {
                    sleep(5);
                    result.add("thenAsync nested promise resolve: " + e);
                    cb.onSuccess(false);
                  });
            })
        .then(
            (value) -> {
              result.add("then onSuccess: " + value);
              latch.countDown();
              return null;
            })
        .doCatch(
            (e) -> {
              result.add("doCatch: " + e);
              return null;
            });

    latch.await();

    assertEquals(
        List.of(
            "resolve",
            "doCatchAsync nested promise resolve: java.lang.RuntimeException",
            "thenAsync nested promise resolve: java.lang.RuntimeException",
            "then onSuccess: false"),
        new ArrayList<>(result));
    threadPool.shutdown();
  }

  @Test
  public void testNestedPromiseOnFinished() throws InterruptedException {
    var threadPool = Executors.newFixedThreadPool(1);
    var latch = new CountDownLatch(1);
    Deque<String> result = new ConcurrentLinkedDeque<>();
    new Promise<Boolean>(
            cb -> {
              threadPool.execute(
                  () -> {
                    sleep(10);
                    result.add("resolve");
                    cb.onFailure(new RuntimeException());
                  });
            })
        .doCatchAsync(
            (e) -> {
              return new Promise<Integer>(
                  (cb) -> {
                    threadPool.execute(
                        () -> {
                          sleep(10);
                          result.add("doCatchAsync nested promise resolve: " + e);
                          cb.onSuccess(2);
                        });
                  });
            })
        .doFinallyAsync(
            () -> {
              return new Promise<Integer>(
                  (cb) -> {
                    threadPool.execute(
                        () -> {
                          sleep(10);
                          result.add("doFinallyAsync nested promise resolve");
                          cb.onSuccess(4);
                        });
                  });
            })
        .then(
            (value) -> {
              result.add("then onSuccess: " + value);
              latch.countDown();
              return null;
            });

    latch.await();

    assertEquals(
        List.of(
            "resolve",
            "doCatchAsync nested promise resolve: java.lang.RuntimeException",
            "doFinallyAsync nested promise resolve",
            "then onSuccess: 4"),
        new ArrayList<>(result));
    threadPool.shutdown();
  }

  @Test
  public void testNestedPromiseOnFailureThenOnFinished() throws InterruptedException {
    var threadPool = Executors.newFixedThreadPool(1);
    var latch = new CountDownLatch(1);
    Deque<String> result = new ConcurrentLinkedDeque<>();
    new Promise<Boolean>(
            cb -> {
              threadPool.execute(
                  () -> {
                    sleep(10);
                    result.add("resolve");
                    cb.onFailure(new RuntimeException());
                  });
            })
        .doFinallyAsync(
            () -> {
              return new Promise<Integer>(
                  (cb) -> {
                    threadPool.execute(
                        () -> {
                          sleep(10);
                          result.add("doFinallyAsync nested promise resolve");
                          cb.onSuccess(4);
                        });
                  });
            })
        .then(
            (value) -> {
              result.add("then onSuccess: " + value);
              return null;
            },
            (e) -> {
              result.add("then onFailure: " + e);
              latch.countDown();
              return null;
            });

    latch.await();

    assertEquals(
        List.of(
            "resolve",
            "doFinallyAsync nested promise resolve",
            "then onFailure: java.lang.RuntimeException"),
        new ArrayList<>(result));
    threadPool.shutdown();
  }

  @Test
  public void testEmptyPromiseNoValue() {
    List<Object> result = new ArrayList<>();
    Promise.emptyPromise().then(result::add);
    assertNull(result.get(0));
  }

  @Test
  public void testEmptyPromise() {
    List<String> result = new ArrayList<>();
    new Promise<String>(
            cb -> {
              cb.onSuccess("empty promise");
            })
        .thenAsync(Promise::emptyPromise)
        .thenAsync(Promise::emptyPromise)
        .thenAsync(Promise::emptyPromise)
        .then(result::add);

    assertEquals(List.of("empty promise"), result);
  }

  @Test
  public void testPromiseAllOnSuccess() throws InterruptedException {
    var threadPool = Executors.newFixedThreadPool(5);
    var latch = new CountDownLatch(1);

    List<Promise<String>> promises = new ArrayList<>();
    List<String> values = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      var value = RandomUtils.randomShortUUID();
      values.add(value);
      if (i % 100 == 0) {
        promises.add(
            new Promise<>(
                (cb) -> {
                  threadPool.execute(
                      () -> {
                        sleep(5);
                        cb.onSuccess(value);
                      });
                }));
      } else {
        promises.add(new Promise<>(cb -> cb.onSuccess(value)));
      }
    }

    Promise.<String>all(
            new Promises<>() {
              @Override
              public List<Promise<String>> list() {
                return promises;
              }

              @Override
              public String[] newResult(int size) {
                return new String[size];
              }
            })
        .then(
            (v) -> {
              assertEquals(values, Arrays.asList(v));
              latch.countDown();
              return null;
            });

    latch.await();

    threadPool.shutdown();
  }

  @Test
  public void testPromiseAllOnFailure() throws InterruptedException {
    var threadPool = Executors.newFixedThreadPool(5);
    var latch = new CountDownLatch(1);

    Promise.<String>all(
            new Promises<>() {
              @Override
              public List<Promise<String>> list() {
                return List.of(
                    new Promise<>((cb) -> cb.onSuccess("1")),
                    new Promise<>(
                        (cb) ->
                            threadPool.execute(
                                () -> {
                                  sleep(10);
                                  cb.onSuccess("2");
                                })),
                    new Promise<>((cb) -> cb.onSuccess("3")),
                    new Promise<>(
                        (cb) ->
                            threadPool.execute(
                                () -> {
                                  sleep(10);
                                  cb.onSuccess("4");
                                })),
                    new Promise<>((cb) -> cb.onFailure(new RuntimeException())));
              }

              @Override
              public String[] newResult(int size) {
                return new String[size];
              }
            })
        .doCatch(
            (e) -> {
              assertTrue(e instanceof RuntimeException);
              latch.countDown();
              return null;
            });

    latch.await();

    threadPool.shutdown();
  }

  @Test
  public void testPromiseChainOnSuccess() {
    List<Integer> result = new ArrayList<>();

    Function<Integer, Promise<Integer>> supplier =
        (v) -> {
          if (v != null && v >= 10) {
            return null;
          }
          return new Promise<Integer>(
              (cb) -> {
                if (v != null) {
                  result.add(v);
                }
                cb.onSuccess(v == null ? 0 : v + 1);
              });
        };

    Promise.chain(supplier).then(result::add);

    assertEquals(List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), result);
  }

  @Test
  public void testPromiseChainOnFailure() {
    List<String> result = new ArrayList<>();

    Function<Integer, Promise<Integer>> supplier =
        (v) -> {
          if (v != null && v >= 10) {
            return null;
          }
          return new Promise<Integer>(
              (cb) -> {
                if (v != null) {
                  if (v == 5) {
                    throw new RuntimeException();
                  }

                  result.add(String.valueOf(v));
                }

                cb.onSuccess(v == null ? 0 : v + 1);
              });
        };

    Promise.chain(supplier)
        .then(
            (v) -> {
              result.add(String.valueOf(v));
              return null;
            },
            (e) -> {
              result.add("" + e);
              return null;
            });

    assertEquals(List.of("0", "1", "2", "3", "4", "java.lang.RuntimeException"), result);
  }
}
