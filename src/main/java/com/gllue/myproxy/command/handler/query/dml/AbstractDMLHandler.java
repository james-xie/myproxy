package com.gllue.myproxy.command.handler.query.dml;

import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.AbstractQueryHandler;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.dml.select.TableScopeFactory;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.core.service.TransportService;
import com.gllue.myproxy.repository.PersistRepository;

public abstract class AbstractDMLHandler<Result extends HandlerResult>
    extends AbstractQueryHandler<Result> {
  protected AbstractDMLHandler(
      final PersistRepository repository,
      final Configurations configurations,
      final ClusterState clusterState,
      final TransportService transportService,
      final SQLParser sqlParser) {
    super(repository, configurations, clusterState, transportService, sqlParser);
  }

  protected TableScopeFactory newScopeFactory(final QueryHandlerRequest request) {
    return new TableScopeFactory(
        request.getDatasource(), request.getDatabase(), clusterState.getMetaData());
  }
}
