package com.gllue.myproxy.command.result.query;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.gllue.myproxy.command.result.query.DefaultQueryResultMetaData.Column;
import com.gllue.myproxy.common.util.RandomUtils;
import com.gllue.myproxy.transport.constant.MySQLColumnType;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MemoryBufferedQueryResultTest extends BaseQueryResultTest {
  @Test
  public void testNext() {
    var columnCount = 3;
    var rowCount = 10;
    var queryResult = new MemoryBufferedQueryResult(prepareMetaData(columnCount));
    var rows = new byte[rowCount][columnCount][];
    for (int i = 0; i < rowCount; i++) {
      rows[i] = buildRandomRow(columnCount);
      queryResult.addRow(rows[i]);
    }
    queryResult.setDone();
    assertRowsEquals(rows, queryResult);
  }

  @Test
  public void testMaxCapacity() {
    var columnCount = 1;
    var rowCount = 10;
    var queryResult = new MemoryBufferedQueryResult(prepareMetaData(columnCount), 10 * 1024);
    var rows = new byte[rowCount][columnCount][];
    for (int i = 0; i < rowCount; i++) {
      rows[i] = buildEmptyRow(columnCount, 1024);
      queryResult.addRow(rows[i]);
    }
    queryResult.setDone();
    assertRowsEquals(rows, queryResult);
  }

  @Test(expected = QueryResultOutOfMemoryException.class)
  public void testBufferOutOfMemory() {
    var columnCount = 101;
    var rowCount = 1;
    var queryResult = new MemoryBufferedQueryResult(prepareMetaData(columnCount), 100);
    var rows = new byte[rowCount][columnCount][];
    for (int i = 0; i < rowCount; i++) {
      rows[i] = buildEmptyRow(columnCount, 1);
      queryResult.addRow(rows[i]);
    }
    queryResult.setDone();
  }

  @Test
  public void testDiscardReadRows() {
    var columnCount = 10;
    var rowCount = 1000;
    var queryResult =
        new MemoryBufferedQueryResult(prepareMetaData(columnCount), 2000, 1000, 1000, 0);
    var rows = new byte[rowCount][columnCount][];
    int readIndex = 0;
    boolean shouldBreakRead = false;
    var random = new Random();
    for (int i = 0; i < rowCount; i++) {
      rows[i] = buildRandomRow(columnCount);
      queryResult.addRow(rows[i]);
      if (i == rowCount - 1) {
        queryResult.setDone();
      }

      if ((i + 1) % 10 == 0) {
        int breakCount = random.nextInt(10);

        while (queryResult.next()) {
          for (int col = 0; col < columnCount; col++) {
            assertArrayEquals(rows[readIndex][col], queryResult.getValue(col));
          }
          readIndex++;

          if ((shouldBreakRead && breakCount-- == 0) || queryResult.readableRows() == 1) {
            break;
          }
        }
        shouldBreakRead = !shouldBreakRead;
        queryResult.discardReadRows();
      }
    }
  }

  @Test
  public void testDiscardSomeReadRows() {
    var columnCount = 10;
    var queryResult =
        new MemoryBufferedQueryResult(prepareMetaData(columnCount), 2000, 1000, 1000, 0);

    int index = 0;
    var rows = new byte[100][columnCount][];
    for (int i = 0; i < 15; i++) {
      rows[i] = buildRandomRow(columnCount);
      queryResult.addRow(rows[i]);
      index++;
    }

    for (int i=0; i<10; i++) {
      queryResult.next();
      for (int col = 0; col < columnCount; col++) {
        assertArrayEquals(rows[i][col], queryResult.getValue(col));
      }
    }
    queryResult.discardSomeReadRows();

    for (int i = 0; i < 10; i++) {
      rows[index] = buildRandomRow(columnCount);
      queryResult.addRow(rows[index]);
      index++;
    }
    queryResult.setDone();

    int readIndex = 10;
    while (queryResult.next()) {
      for (int col = 0; col < columnCount; col++) {
        assertArrayEquals(rows[readIndex][col], queryResult.getValue(col));
      }
      readIndex++;
    }
  }

  @Test
  public void testWritabilityChanged() {
    var columnCount = 1;
    var queryResult =
        new MemoryBufferedQueryResult(prepareMetaData(columnCount), 3000, 1000, 2000, 3000);
    AtomicBoolean writable = new AtomicBoolean(false);
    queryResult.setWritabilityChangedListener(() -> writable.set(true));
    assertFalse(queryResult.addRow(buildEmptyRow(columnCount, 500)));
    assertFalse(queryResult.addRow(buildEmptyRow(columnCount, 500)));
    assertFalse(queryResult.addRow(buildEmptyRow(columnCount, 500)));
    assertFalse(queryResult.addRow(buildEmptyRow(columnCount, 500)));
    assertTrue(queryResult.addRow(buildEmptyRow(columnCount, 500)));
    queryResult.setDone();
    queryResult.next();
    queryResult.next();
    queryResult.next();
    queryResult.next();
    queryResult.discardReadRows();
    assertFalse(writable.get());
    queryResult.next();
    queryResult.discardReadRows();
    assertTrue(writable.get());
  }

  @Test
  public void testReadAndWriteInDiffThreads() throws InterruptedException {
    var columnCount = 1;
    var rowCount = 50000;
    var queryResult = new MemoryBufferedQueryResult(prepareMetaData(columnCount));
    var rows = new byte[rowCount][columnCount][];
    var latch = new CountDownLatch(1);

    var t1 =
        new Thread(
            () -> {
              int i = 0;
              while (queryResult.next()) {
                for (int col = 0; col < columnCount; col++) {
                  assertArrayEquals(rows[i][col], queryResult.getValue(col));
                }
                i++;
              }
              assertEquals(rows.length, i);
              latch.countDown();
            });
    var t2 =
        new Thread(
            () -> {
              try {
                var random = new Random();
                var sleepCount = random.nextInt(500) + 1;
                for (int i = 0; i < rowCount; i++) {
                  rows[i] = buildRandomRow(columnCount);
                  queryResult.addRow(rows[i]);
                  if (i % sleepCount == 0) {
                    Thread.sleep(1);
                    sleepCount = random.nextInt(500) + 1;
                  }
                }
                queryResult.setDone();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            });
    t1.start();
    t2.start();
    latch.await();
  }
}
