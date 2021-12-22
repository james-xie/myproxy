package com.gllue.myproxy.cluster;

import com.gllue.myproxy.bootstrap.ServerContext;
import com.gllue.myproxy.common.Initializer;
import com.gllue.myproxy.common.util.PathUtils;
import com.gllue.myproxy.constant.ServerConstants;
import com.gllue.myproxy.metadata.loader.MultiDatabasesMetaDataLoader;
import com.gllue.myproxy.metadata.model.MultiDatabasesMetaData;
import com.gllue.myproxy.metadata.watch.MultiDatabasesMetaDataWatcher;
import com.gllue.myproxy.repository.ClusterPersistRepository;
import com.gllue.myproxy.repository.PersistRepository;

public class ClusterStateInitializer implements Initializer {

  @Override
  public String name() {
    return "cluster state";
  }

  private void watchDatabasesMetaDataChangeEvent(
      PersistRepository repository, String basePath, MultiDatabasesMetaData metaData) {
    if (repository instanceof ClusterPersistRepository) {
      var clusterRepo = (ClusterPersistRepository) repository;
      new MultiDatabasesMetaDataWatcher(basePath, metaData, clusterRepo).watch();
    }
  }

  @Override
  public void initialize(ServerContext context) {
    var loader = new MultiDatabasesMetaDataLoader(context.getPersistRepository());
    var rootPath = PathUtils.getRootPath(context.getConfigurations());
    var dbBasePath = PathUtils.joinPaths(rootPath, ServerConstants.DATABASES_ROOT_PATH);
    var databasesMetaData = loader.load(dbBasePath);
    // todo: read nodes info from the repository;
    var node = new ClusterNode(1);
    var clusterState = new ClusterState(node, new ClusterNode[] {node}, databasesMetaData);
    watchDatabasesMetaDataChangeEvent(context.getPersistRepository(), dbBasePath, databasesMetaData);
    context.setClusterState(clusterState);
  }

  @Override
  public void close() throws Exception {}
}
