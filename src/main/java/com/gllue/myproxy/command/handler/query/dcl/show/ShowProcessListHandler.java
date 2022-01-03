package com.gllue.myproxy.command.handler.query.dcl.show;

import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.AbstractQueryHandler;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.QueryHandlerResult;
import com.gllue.myproxy.command.result.query.QueryResult;
import com.gllue.myproxy.command.result.query.SimpleQueryResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.common.util.NetworkUtils;
import com.gllue.myproxy.transport.core.service.TransportService;
import com.gllue.myproxy.transport.core.service.TransportService.ConnectionInfo;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowProcessListHandler extends AbstractQueryHandler {
  private static final String NAME = "Show process list handler";
  private static final int ID_INDEX = 0;
  private static final int USER_INDEX = 1;
  private static final int HOST_INDEX = 2;

  public ShowProcessListHandler(
      final TransportService transportService, final ThreadPool threadPool) {
    super(transportService, threadPool);
  }

  @Override
  public String name() {
    return NAME;
  }

  private void updateRow(String[] row, Map<Integer, ConnectionInfo> connectionInfoMap) {
    var id = Integer.valueOf(row[ID_INDEX]);
    if (!connectionInfoMap.containsKey(id)) {
      return;
    }

    var connectionInfo = connectionInfoMap.get(id);
    row[ID_INDEX] = String.valueOf(connectionInfo.getFrontendConnectionId());
    row[USER_INDEX] = String.format("%s (proxy)", connectionInfo.getUser());
    row[HOST_INDEX] = NetworkUtils.toAddressString(connectionInfo.getClientSocketAddress());
  }

  private QueryResult updateShowProcessListResult(String datasource, QueryResult result) {
    assert "Id".equalsIgnoreCase(result.getMetaData().getColumnLabel(ID_INDEX));
    assert "User".equalsIgnoreCase(result.getMetaData().getColumnLabel(USER_INDEX));
    assert "Host".equalsIgnoreCase(result.getMetaData().getColumnLabel(HOST_INDEX));

    var connectionInfos = getConnectionInfoList(datasource);
    var connectionInfoMap =
        connectionInfos.stream()
            .collect(Collectors.toMap(ConnectionInfo::getBackendConnectionId, (x) -> x));

    var rows = new ArrayList<String[]>();
    var columnCount = result.getMetaData().getColumnCount();
    while (result.next()) {
      var row = new String[columnCount];
      for (int i = 0; i < columnCount; i++) {
        row[i] = result.getStringValue(i);
      }
      updateRow(row, connectionInfoMap);
      rows.add(row);
    }
    return new SimpleQueryResult(result.getMetaData(), rows);
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    submitQueryToBackendDatabase(request.getConnectionId(), request.getQuery())
        .then(
            (result) -> {
              var queryResult =
                  updateShowProcessListResult(request.getDatasource(), result.getQueryResult());
              callback.onSuccess(new QueryHandlerResult(queryResult));
              return true;
            })
        .doCatch(
            (e) -> {
              callback.onFailure(e);
              return false;
            });
  }
}
