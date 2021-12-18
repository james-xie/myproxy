package com.gllue.myproxy.command.handler.query.dml.select;

import com.gllue.myproxy.cluster.ClusterState;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.dml.AbstractDMLHandler;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.config.Configurations;
import com.gllue.myproxy.repository.PersistRepository;
import com.gllue.myproxy.sql.parser.SQLParser;
import com.gllue.myproxy.transport.core.service.TransportService;

public class SelectQueryHandler extends AbstractDMLHandler<SelectQueryResult> {
  private static final String NAME = "Select query handler";

  public SelectQueryHandler(
      PersistRepository repository,
      Configurations configurations,
      ClusterState clusterState,
      TransportService transportService,
      SQLParser sqlParser) {
    super(repository, configurations, clusterState, transportService, sqlParser);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<SelectQueryResult> callback) {

  }
}
