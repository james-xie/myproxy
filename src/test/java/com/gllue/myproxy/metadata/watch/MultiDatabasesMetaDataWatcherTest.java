package com.gllue.myproxy.metadata.watch;

import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

import com.gllue.myproxy.common.io.stream.ByteArrayStreamOutput;
import com.gllue.myproxy.common.util.PathUtils;
import com.gllue.myproxy.metadata.codec.DatabaseMetaDataCodec;
import com.gllue.myproxy.metadata.codec.MetaDataCodec;
import com.gllue.myproxy.metadata.model.DatabaseMetaData;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
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

  DatabaseMetaData prepareDatabase(String name) {
    var builder = new DatabaseMetaData.Builder();
    builder.setDatasource(DATASOURCE);
    builder.setName(name);
    return builder.build();
  }

  byte[] prepareDatabaseStreamBytes(String name) {
    var database = prepareDatabase(name);
    var streamOutput = new ByteArrayStreamOutput();
    getCodec().encode(streamOutput, database);
    return streamOutput.getTrimmedByteArray();
  }

  String dbKey(final String dbName) {
    return DatabaseMetaData.joinDatasourceAndName(DATASOURCE, dbName);
  }

  MetaDataCodec<DatabaseMetaData> getCodec() {
    return DatabaseMetaDataCodec.getInstance();
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
}
