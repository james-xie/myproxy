package com.gllue.metadata.command;

import com.gllue.common.io.stream.ByteArrayStreamOutput;
import com.gllue.common.util.PathUtils;
import com.gllue.constant.ServerConstants;
import com.gllue.metadata.MetaData;
import com.gllue.metadata.command.context.CommandExecutionContext;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractMetaDataCommand<T extends MetaData> implements MetaDataCommand<T> {
  String getPersistPathForMetaData(
      final CommandExecutionContext<T> context, MetaData... metaDataArray) {
    List<String> items = new ArrayList<>();
    items.add(PathUtils.getRootPath(context.getConfigurations()));
    items.add(ServerConstants.DATABASES_ROOT_PATH);
    for (var metadata: metaDataArray) {
      items.add(metadata.getIdentity());
    }
    return String.join(ServerConstants.PATH_SEPARATOR, items);
  }

  void saveMetaData(CommandExecutionContext<T> context, String path, MetaData metaData) {
    var stream = new ByteArrayStreamOutput();
    var repository = context.getRepository();
    metaData.writeTo(stream);
    repository.save(path, stream.getTrimmedByteArray());
  }

  void deleteMetaData(CommandExecutionContext<T> context, String path) {
    context.getRepository().delete(path);
  }
}
