package com.gllue.myproxy.command.handler.query.dml;

import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.SchemaRelatedQueryHandler;
import com.gllue.myproxy.command.handler.query.dml.select.TableScopeFactory;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.transport.core.service.TransportService;

public abstract class AbstractDMLHandler extends SchemaRelatedQueryHandler {
  protected AbstractDMLHandler(
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final TransportService transportService,
      final ThreadPool threadPool) {
    super(repository, configurations, clusterState, transportService, threadPool);
  }

  protected TableScopeFactory newScopeFactory(final QueryHandlerRequest request) {
    return new TableScopeFactory(
        request.getDatasource(), request.getDatabase(), clusterState.getMetaData());
  }
}
