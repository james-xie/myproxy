package com.gllue.myproxy.metadata.command;

import com.gllue.myproxy.metadata.MetaDataBuilder.CopyOptions;
import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.model.PartitionTableMetaData;
import com.gllue.myproxy.metadata.model.TableMetaData;
import com.gllue.myproxy.metadata.model.TableType;
import com.google.common.base.Preconditions;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class RenameTableCommand extends SchemaRelatedMetaDataCommand {
  private final String datasource;
  private final String fromDatabaseName;
  private final String toDatabaseName;
  private final String oldName;
  private final String newName;

  @Override
  public void execute(CommandExecutionContext<MultiDatabasesMetaData> context) {
    Preconditions.checkArgument(
        oldName != null && newName != null && !oldName.equals(newName),
        "Bad table name. [old:%s, new:%s]",
        oldName,
        newName);

    var metadata = context.getRootMetaData();
    var fromDatabase = metadata.getDatabase(datasource, fromDatabaseName);
    Preconditions.checkArgument(
        fromDatabase != null, "Unknown database name. [%s]", fromDatabaseName);
    var toDatabase = metadata.getDatabase(datasource, toDatabaseName);
    Preconditions.checkArgument(
        toDatabase != null, "Unknown database name. [%s]", fromDatabaseName);

    var table = fromDatabase.getTable(oldName);
    Preconditions.checkArgument(table != null, "Unknown table name. [%s]", oldName);
    Preconditions.checkArgument(
        table.getType() == TableType.PARTITION || table.getType() == TableType.STANDARD,
        "Unknown table type. [%s]",
        table.getType());
    Preconditions.checkArgument(
        !toDatabase.hasTable(newName), "Table already exists. [%s]", newName);

    TableMetaData newTable;
    if (table.getType() == TableType.STANDARD) {
      var builder = new TableMetaData.Builder();
      builder.copyFrom(table, CopyOptions.COPY_CHILDREN);
      builder.setNextVersion(table.getVersion());
      builder.setName(newName);
      newTable = builder.build();
    } else {
      var builder = new PartitionTableMetaData.Builder();
      builder.copyFrom((PartitionTableMetaData) table, CopyOptions.COPY_CHILDREN);
      builder.setNextVersion(table.getVersion());
      builder.setName(newName);
      newTable = builder.build();
    }

    if (fromDatabaseName.equals(toDatabaseName)) {
      var databaseBuilder = newDatabaseBuilder(fromDatabase);
      databaseBuilder.removeTable(table.getIdentity());
      databaseBuilder.addTable(newTable);
      refreshAndSaveDatabase(context, databaseBuilder.build());
    } else {
      var fromDatabaseBuilder = newDatabaseBuilder(fromDatabase);
      var toDatabaseBuilder = newDatabaseBuilder(toDatabase);
      fromDatabaseBuilder.removeTable(table.getIdentity());
      toDatabaseBuilder.addTable(newTable);
      refreshAndSaveDatabase(context, fromDatabaseBuilder.build());
      refreshAndSaveDatabase(context, toDatabaseBuilder.build());
    }
  }
}
