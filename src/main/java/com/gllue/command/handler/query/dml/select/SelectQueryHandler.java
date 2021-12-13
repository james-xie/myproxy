package com.gllue.command.handler.query.dml.select;

import com.gllue.cluster.ClusterState;
import com.gllue.command.handler.query.QueryHandlerRequest;
import com.gllue.command.handler.query.dml.AbstractDMLHandler;
import com.gllue.common.Callback;
import com.gllue.config.Configurations;
import com.gllue.repository.PersistRepository;
import com.gllue.sql.parser.SQLParser;
import com.gllue.transport.core.service.TransportService;

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
