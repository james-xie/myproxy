package com.gllue.myproxy.metadata.command;

import com.gllue.myproxy.common.util.PathUtils;
import com.gllue.myproxy.constant.ServerConstants;
import com.gllue.myproxy.metadata.MetaData;
import com.gllue.myproxy.metadata.command.context.CommandExecutionContext;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class AbstractMetaDataCommand<T extends MetaData> implements MetaDataCommand<T> {
  String getPersistPathForMetaData(
      final CommandExecutionContext<T> context, MetaData... metaDataArray) {
    List<String> items = new ArrayList<>();
    items.add(PathUtils.getRootPath(context.getConfigurations()));
    items.add(ServerConstants.DATABASES_ROOT_PATH);
    for (var metadata : metaDataArray) {
      items.add(metadata.getIdentity());
    }
    return String.join(ServerConstants.PATH_SEPARATOR, items);
  }

  void saveMetaData(CommandExecutionContext<T> context, String path, byte[] data) {
    var repository = context.getRepository();
    if (log.isDebugEnabled()) {
      log.debug("Persist meta data for path [{}].", path);
    }
    repository.save(path, data);
  }

  //  void saveMetaData(CommandExecutionContext<T> context, String path, MetaData metaData) {
  //    var stream = new ByteArrayStreamOutput();
  //    var repository = context.getRepository();
  //    metaData.writeTo(stream);
  //    if (log.isDebugEnabled()) {
  //      log.debug("Persist meta data for path [{}].", path);
  //    }
  //    repository.save(path, stream.getTrimmedByteArray());
  //  }

  void deleteMetaData(CommandExecutionContext<T> context, String path) {
    context.getRepository().delete(path);
  }
}
