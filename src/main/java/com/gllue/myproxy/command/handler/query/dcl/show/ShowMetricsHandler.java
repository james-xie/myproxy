package com.gllue.myproxy.command.handler.query.dcl.show;

import com.gllue.myproxy.command.handler.HandlerResult;
import com.gllue.myproxy.command.handler.query.AbstractQueryHandler;
import com.gllue.myproxy.command.handler.query.QueryHandlerRequest;
import com.gllue.myproxy.command.handler.query.QueryHandlerResult;
import com.gllue.myproxy.command.result.query.DefaultQueryResultMetaData;
import com.gllue.myproxy.command.result.query.DefaultQueryResultMetaData.Column;
import com.gllue.myproxy.command.result.query.QueryResult;
import com.gllue.myproxy.command.result.query.SimpleQueryResult;
import com.gllue.myproxy.common.Callback;
import com.gllue.myproxy.common.concurrent.ThreadPool;
import com.gllue.myproxy.transport.constant.MySQLColumnType;
import com.gllue.myproxy.transport.core.service.TransportService;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.CollectorRegistry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;

public class ShowMetricsHandler extends AbstractQueryHandler {
  public static final String NAME = "Show metrics handler";
  private static final DefaultQueryResultMetaData.Column[] columns =
      new Column[] {
        new Column("name", MySQLColumnType.MYSQL_TYPE_VAR_STRING),
        new Column("unit", MySQLColumnType.MYSQL_TYPE_VAR_STRING),
        new Column("type", MySQLColumnType.MYSQL_TYPE_VAR_STRING),
        new Column("help", MySQLColumnType.MYSQL_TYPE_VAR_STRING),
        new Column("samples", MySQLColumnType.MYSQL_TYPE_VAR_STRING)
      };

  public ShowMetricsHandler(TransportService transportService, ThreadPool threadPool) {
    super(transportService, threadPool);
  }

  @Override
  public String name() {
    return NAME;
  }

  private String formatSamples(List<Sample> samples) {
    var sb = new StringBuilder();
    for (var sample : samples) {
      sb.append("\n        {");
      sb.append(sample.toString());
      sb.append("}");
    }
    return sb.toString();
  }

  private String[][] buildMetricRows(Enumeration<MetricFamilySamples> metrics) {
    var rows = new ArrayList<String[]>();
    while (metrics.hasMoreElements()) {
      var metric = metrics.nextElement();
      rows.add(
          new String[] {
            metric.name,
            metric.unit,
            metric.type.toString(),
            metric.help,
            formatSamples(metric.samples)
          });
    }
    rows.sort(Comparator.comparing(a -> a[0]));
    return rows.toArray(new String[0][]);
  }

  private QueryResult newMetricResult(Enumeration<MetricFamilySamples> metrics) {
    return new SimpleQueryResult(new DefaultQueryResultMetaData(columns), buildMetricRows(metrics));
  }

  @Override
  public void execute(QueryHandlerRequest request, Callback<HandlerResult> callback) {
    var registry = CollectorRegistry.defaultRegistry;
    var metrics = registry.metricFamilySamples();
    callback.onSuccess(new QueryHandlerResult(newMetricResult(metrics)));
  }
}
