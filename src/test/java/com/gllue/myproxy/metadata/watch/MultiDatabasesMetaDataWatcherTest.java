package com.gllue.myproxy.metadata.watch;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.gllue.myproxy.common.io.stream.ByteArrayStreamOutput;
import com.gllue.myproxy.common.util.PathUtils;
import com.gllue.myproxy.metadata.model.ColumnType;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.gllue.myproxy.metadata.model.ColumnMetaData.Builder;
import com.gllue.myproxy.repository.ClusterPersistRepository;
import com.gllue.myproxy.repository.DataChangedEvent;
import com.gllue.myproxy.repository.DataChangedEvent.Type;
import com.gllue.myproxy.repository.DataChangedEventListener;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MultiDatabasesMetaDataWatcherTest {
  static final String DATASOURCE = "ds";
  static final String BASE_PATH = "/root";

  @Mock MultiDatabasesMetaData databases;

  @Mock DatabaseMetaData database;

  @Mock ClusterPersistRepository repository;

  AtomicReference<DataChangedEventListener> mockListener() {
    var listenerRef = new AtomicReference<DataChangedEventListener>();
    doAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              listenerRef.set((DataChangedEventListener) args[1]);
              return null;
            })
        .when(repository)
        .watch(anyString(), any());
    return listenerRef;
  }

  MultiDatabasesMetaDataWatcher prepareWatcher() {
    return new MultiDatabasesMetaDataWatcher(BASE_PATH, databases, repository);
  }

  String concatPath(String... path) {
    return PathUtils.joinPaths(path);
  }

  TableMetaData prepareTable(String name) {
    var builder = new TableMetaData.Builder();
    builder.setName(name).setType(TableType.PRIMARY).setIdentity(name).setVersion(1);
    builder.addColumn(
        new Builder().setName("col").setType(ColumnType.VARCHAR).build());
    return builder.build();
  }

  DatabaseMetaData prepareDatabase(String name) {
    var builder = new DatabaseMetaData.Builder();
    builder.setDatasource(DATASOURCE);
    builder.setName(name);
    return builder.build();
  }

  byte[] prepareDatabaseStreamBytes(String name) {
    var database = prepareDatabase(name);
    var streamOutput = new ByteArrayStreamOutput();
    database.writeTo(streamOutput);
    return streamOutput.getTrimmedByteArray();
  }

  byte[] prepareTableStreamBytes(String name) {
    var table = prepareTable(name);
    var streamOutput = new ByteArrayStreamOutput();
    table.writeTo(streamOutput);
    return streamOutput.getTrimmedByteArray();
  }

  String dbKey(final String dbName) {
    return DatabaseMetaData.joinDatasourceAndName(DATASOURCE, dbName);
  }

  @Test
  public void testDispatchDatabaseCreateEvent() {
    var listenerRef = mockListener();
    var watcher = prepareWatcher();
    watcher.watch();

    var dbName = "db";
    var dbKey = dbKey(dbName);
    var key = concatPath(BASE_PATH, dbKey);
    var value = prepareDatabaseStreamBytes(dbName);
    assertNotNull(listenerRef.get());
    listenerRef.get().onChange(new DataChangedEvent(key, value, Type.CREATED));
    verify(databases).addDatabase(any(), eq(false));
  }

  @Test
  public void testDispatchDatabaseUpdateEvent() {
    var listenerRef = mockListener();
    var watcher = prepareWatcher();
    watcher.watch();

    var dbName = "db";
    var dbKey = dbKey(dbName);
    var key = concatPath(BASE_PATH, dbKey);
    var value = prepareDatabaseStreamBytes(dbName);
    assertNotNull(listenerRef.get());
    listenerRef.get().onChange(new DataChangedEvent(key, value, Type.UPDATED));
    verify(databases).addDatabase(any(), eq(true));
  }

  @Test
  public void testDispatchDatabaseDeleteEvent() {
    var listenerRef = mockListener();
    var watcher = prepareWatcher();
    watcher.watch();

    var dbName = "db";
    var dbKey = dbKey(dbName);
    var key = concatPath(BASE_PATH, dbKey);
    var value = prepareDatabaseStreamBytes(dbName);
    assertNotNull(listenerRef.get());
    listenerRef.get().onChange(new DataChangedEvent(key, value, Type.DELETED));
    verify(databases).removeDatabase(eq(DATASOURCE), eq(dbName));
  }

  @Test
  public void testDispatchTableCreateEvent() {
    var listenerRef = mockListener();
    var watcher = prepareWatcher();
    watcher.watch();

    var dbName = "db";
    var dbKey = dbKey(dbName);
    var tableName = "table";
    var key = concatPath(BASE_PATH, dbKey, tableName);
    var value = prepareTableStreamBytes(tableName);
    assertNotNull(listenerRef.get());

    when(databases.getDatabase(DATASOURCE, dbName)).thenReturn(database);
    listenerRef.get().onChange(new DataChangedEvent(key, value, Type.CREATED));
    verify(database).addTable(any(), eq(false));
  }

  @Test
  public void testDispatchTableUpdateEvent() {
    var listenerRef = mockListener();
    var watcher = prepareWatcher();
    watcher.watch();

    var dbName = "db";
    var dbKey = dbKey(dbName);
    var tableName = "table";
    var key = concatPath(BASE_PATH, dbKey, tableName);
    var value = prepareTableStreamBytes(tableName);
    assertNotNull(listenerRef.get());

    when(databases.getDatabase(DATASOURCE, dbName)).thenReturn(database);
    listenerRef.get().onChange(new DataChangedEvent(key, value, Type.UPDATED));
    verify(database).addTable(any(), eq(true));
  }

  @Test
  public void testDispatchTableDeleteEvent() {
    var listenerRef = mockListener();
    var watcher = prepareWatcher();
    watcher.watch();

    var dbName = "db";
    var tableName = "table";
    var dbKey = dbKey(dbName);
    var key = concatPath(BASE_PATH, dbKey, tableName);
    assertNotNull(listenerRef.get());

    when(databases.getDatabase(DATASOURCE, dbName)).thenReturn(database);
    listenerRef.get().onChange(new DataChangedEvent(key, null, Type.DELETED));
    verify(database).removeTable(tableName);
  }
}
