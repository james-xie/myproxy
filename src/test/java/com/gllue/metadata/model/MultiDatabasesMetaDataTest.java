package com.gllue.metadata.model;

import static org.junit.Assert.assertEquals;

import com.gllue.common.io.stream.ByteArrayStreamInput;
import com.gllue.common.io.stream.ByteArrayStreamOutput;
import com.gllue.metadata.MetaDataBuilder.CopyOptions;
import java.util.HashSet;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MultiDatabasesMetaDataTest {
  static final String DATASOURCE = "ds";

  DatabaseMetaData prepareDatabase(String name) {
    var builder = new DatabaseMetaData.Builder();
    builder.setDatasource(DATASOURCE);
    builder.setName(name);
    return builder.build();
  }

  MultiDatabasesMetaData prepareMultiDatabases() {
    var builder = new MultiDatabasesMetaData.Builder();
    builder.addDatabase(prepareDatabase("db1"));
    builder.addDatabase(prepareDatabase("db2"));
    builder.addDatabase(prepareDatabase("db3"));
    return builder.build();
  }

  Set<String> getDatabaseNames(MultiDatabasesMetaData multiDatabases) {
    var names = new HashSet<String>();
    for (var name : multiDatabases.getDatabaseNames(DATASOURCE)) {
      names.add(name);
    }
    return names;
  }

  @Test
  public void testBuild() {
    var multiDatabases = prepareMultiDatabases();
    assertEquals(Set.of("db1", "db2", "db3"), getDatabaseNames(multiDatabases));
  }

  @Test
  public void testCopyFrom() {
    var multiDatabases = prepareMultiDatabases();
    var builder = new MultiDatabasesMetaData.Builder();
    builder.copyFrom(multiDatabases, CopyOptions.COPY_CHILDREN);
    var newMultiDatabases = builder.build();

    assertEquals(multiDatabases.getIdentity(), newMultiDatabases.getIdentity());
    assertEquals(multiDatabases.getVersion(), newMultiDatabases.getVersion());
    assertEquals(getDatabaseNames(multiDatabases), getDatabaseNames(newMultiDatabases));
  }
}
