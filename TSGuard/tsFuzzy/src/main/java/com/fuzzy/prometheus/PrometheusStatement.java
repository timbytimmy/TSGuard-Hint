package com.fuzzy.prometheus;

import com.alibaba.fastjson.JSONObject;
import com.benchmark.commonClass.TSFuzzyStatement;
import com.benchmark.entity.DBValResultSet;
import com.fuzzy.prometheus.apiEntry.*;
import com.fuzzy.prometheus.apiEntry.entity.CollectorAttribute;
import com.fuzzy.prometheus.client.builder.*;
import com.fuzzy.prometheus.client.builder.pushGateway.PushGatewaySeriesDeleteBuilder;
import com.fuzzy.prometheus.client.builder.pushGateway.PushGatewaySeriesQueryBuilder;
import com.fuzzy.prometheus.resultSet.PrometheusResultSet;
import com.fuzzy.prometheus.util.WaitPrometheusScrapeData;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.PushGateway;
import lombok.extern.slf4j.Slf4j;

import java.net.URI;
import java.sql.SQLException;
import java.util.Map.Entry;

@Slf4j
public class PrometheusStatement implements TSFuzzyStatement {
    private PrometheusApiEntry apiEntry;

    public PrometheusStatement(PrometheusApiEntry apiEntry) {
        this.apiEntry = apiEntry;
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public void execute(String queryParam) throws SQLException {
        try {
            PrometheusRequestParam requestParam = JSONObject.parseObject(queryParam, PrometheusRequestParam.class);
            if (requestParam.getType().isPushData()) {
                CollectorRegistry.defaultRegistry.clear();
                PrometheusInsertParam param = JSONObject.parseObject(queryParam, PrometheusInsertParam.class);
                PushGateway pushGateway = new PushGateway(apiEntry.getPushGatewaySocket());
                for (Entry<String, CollectorAttribute> entry : param.getCollectorList().entrySet()) {
                    pushGateway.push(entry.getValue().transToCollector(), entry.getKey());
                }
                // 休眠1s, 等待Prometheus抓取
                WaitPrometheusScrapeData.waitPrometheusScrapeData();
                return;
            }

            PrometheusQueryParam param = JSONObject.parseObject(queryParam, PrometheusQueryParam.class);
            switch (param.getType()) {
                case series_delete:
                    SeriesDeleteBuilder deleteBuilder =
                            QueryBuilderType.SeriesDelete.newInstance(apiEntry.getTargetServer());
                    URI targetUri = deleteBuilder.withSelector(param.getRequestBody()).build();
                    this.apiEntry.executePostRequest(targetUri);
                    CleanTombstonesBuilder cleanTombstonesBuilder =
                            QueryBuilderType.CleanTombstones.newInstance(apiEntry.getTargetServer());
                    this.apiEntry.executePostRequest(cleanTombstonesBuilder.build());
                    return;
                case push_gateway_series_delete:
                    PushGatewaySeriesDeleteBuilder pushGatewaySeriesDeleteBuilder =
                            QueryBuilderType.PushGatewaySeriesDelete.newInstance(apiEntry.getPushGatewayServer());
                    this.apiEntry.executeDeleteRequest(pushGatewaySeriesDeleteBuilder
                            .withJobName(param.getRequestBody()).build());
                    return;
                default:
                    throw new IllegalArgumentException(String.format("查询参数类型不存在, type:%s",
                            param.getType()));
            }
        } catch (Exception e) {
            log.error("执行操作失败, e:", e);
            throw new SQLException(e.getMessage());
        }
    }

    @Override
    public DBValResultSet executeQuery(String queryParam) throws SQLException {
        try {
            // 参数解析
            PrometheusRequestParam requestParam = JSONObject.parseObject(queryParam, PrometheusRequestParam.class);
            if (requestParam.getType().isPushData()) {
                throw new UnsupportedOperationException();
            }

            PrometheusQueryParam param = JSONObject.parseObject(queryParam, PrometheusQueryParam.class);
            URI targetUri;
            // 依据查询参数调用不同接口请求
            switch (param.getType()) {
                case series_query:
                    SeriesMetaQueryBuilder queryBuilder =
                            QueryBuilderType.SeriesMetadaQuery.newInstance(apiEntry.getTargetServer());
                    if (param.getStart() != null) queryBuilder.withStartEpochTime(param.getStart());
                    if (param.getEnd() != null) queryBuilder.withEndEpochTime(param.getEnd());
                    targetUri = queryBuilder.withSelector(param.getRequestBody()).build();
                    break;
                case push_gateway_series_query:
                    PushGatewaySeriesQueryBuilder pushGatewaySeriesQueryBuilder =
                            QueryBuilderType.PushGatewaySeriesQuery.newInstance(apiEntry.getPushGatewayServer());
                    targetUri = pushGatewaySeriesQueryBuilder.build();
                    break;
//                case series_delete:
//                    SeriesDeleteBuilder deleteBuilder =
//                            QueryBuilderType.SeriesDelete.newInstance(apiEntry.getTargetServer());
//                    targetUri = deleteBuilder.withJobName(param.getRequestBody()).build();
//                    break;
                default:
                    throw new IllegalArgumentException(String.format("查询参数类型不存在, type:%s",
                            param.getType()));
            }
            return new PrometheusResultSet(this.apiEntry.executeGetRequest(targetUri));
        } catch (Exception e) {
            log.error("执行查询失败, queryParam:{} e:", queryParam, e);
            throw new SQLException(e.getMessage());
        }
    }
}
