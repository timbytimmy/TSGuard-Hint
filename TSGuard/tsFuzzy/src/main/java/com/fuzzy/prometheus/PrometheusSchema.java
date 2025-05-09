package com.fuzzy.prometheus;


import com.alibaba.fastjson.JSONObject;
import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.Randomly;
import com.fuzzy.SQLConnection;
import com.fuzzy.common.schema.*;
import com.fuzzy.prometheus.PrometheusSchema.PrometheusTable;
import com.fuzzy.prometheus.apiEntry.PrometheusQueryParam;
import com.fuzzy.prometheus.apiEntry.PrometheusRequestType;
import com.fuzzy.prometheus.apiEntry.entity.PrometheusSeriesResultItem;
import com.fuzzy.prometheus.constant.PrometheusLabelConstant;
import com.fuzzy.prometheus.grpc.PrometheusConstant;
import com.fuzzy.prometheus.resultSet.PrometheusResultSet;
import com.fuzzy.prometheus.util.WaitPrometheusScrapeData;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.*;
import java.util.stream.Collectors;

public class PrometheusSchema extends AbstractSchema<PrometheusGlobalState, PrometheusTable> {

    private static final int NR_SCHEMA_READ_TRIES = 10;

    public enum PrometheusDataType {
        COUNTER, GAUGE, HISTOGRAM, SUMMARY;

        public static PrometheusDataType getRandom(PrometheusGlobalState globalState) {
            if (globalState.usesPQS()) {
                return Randomly.fromOptions(PrometheusDataType.COUNTER, PrometheusDataType.GAUGE);
            } else {
                return Randomly.fromOptions(values());
            }
        }

        public boolean isNumeric() {
            switch (this) {
                case COUNTER:
                case GAUGE:
                case HISTOGRAM:
                case SUMMARY:
                    return true;
                default:
                    throw new AssertionError(this);
            }
        }

    }

    public static class PrometheusColumn extends AbstractTableColumn<PrometheusTable, PrometheusDataType> {

        public PrometheusColumn(String name, boolean isTag, PrometheusDataType type) {
            super(name, null, type, isTag);
        }

    }

    public static class PrometheusTables extends AbstractTables<PrometheusTable, PrometheusColumn> {

        public PrometheusTables(List<PrometheusTable> tables) {
            super(tables);
        }

        public PrometheusRowValue getRandomRowValue(SQLConnection con) throws SQLException {
            return null;
        }

    }

    public static class PrometheusRowValue extends AbstractRowValue<PrometheusTables, PrometheusColumn, PrometheusConstant> {

        PrometheusRowValue(PrometheusTables tables, Map<PrometheusColumn, PrometheusConstant> values) {
            super(tables, values);
        }

    }

    public static class PrometheusTable extends AbstractRelationalTable<PrometheusColumn, PrometheusIndex, PrometheusGlobalState> {

        public PrometheusTable(String name, String databaseName, List<PrometheusColumn> columns, List<PrometheusIndex> indexes) {
            super(name, databaseName, columns, indexes, false);
        }

    }

    public static final class PrometheusIndex extends TableIndex {

        protected PrometheusIndex(String indexName) {
            super(indexName);
        }

    }

    public static PrometheusSchema fromConnection(SQLConnection con, String databaseName) throws SQLException {
        Exception ex = null;
        // 查询表结构 -> show series
        for (int i = 0; i < NR_SCHEMA_READ_TRIES; i++) {
            try {
                List<PrometheusTable> databaseTables = new ArrayList<>();
                try (TSFuzzyStatement s = con.createStatement()) {
                    // 等待创建数据库并被Prometheus抓捕
                    WaitPrometheusScrapeData.waitPrometheusScrapeData();
                    String databaseMatch = String.format("match[]={database=\"%s\"}", databaseName);
                    // 查询数据库结构
                    try (PrometheusResultSet prometheusResultSet = (PrometheusResultSet)
                            s.executeQuery(JSONObject.toJSONString(new PrometheusQueryParam(
                                    PrometheusRequestType.series_query, databaseMatch,
                                    System.currentTimeMillis() / 1000)))) {

                        // 将获取数据按照<database, table, column>去重
                        Set<PrometheusSeriesResultItem> seriesSet = new HashSet<>();
                        while (prometheusResultSet.hasNext()) {
                            Object rowRecord = prometheusResultSet.getCurrentValue();
                            PrometheusSeriesResultItem seriesResultItem =
                                    JSONObject.parseObject(rowRecord.toString(), PrometheusSeriesResultItem.class);
                            if (seriesResultItem.getTable().equals(PrometheusLabelConstant.DATABASE_INIT.getLabel()))
                                continue;

                            if (seriesSet.contains(seriesResultItem)) continue;
                            else seriesSet.add(seriesResultItem);
                        }

                        // tableName -> columns
                        Map<String, List<PrometheusColumn>> tableToSeriesMap = new HashMap<>();
                        seriesSet.forEach(item -> {
                            if (tableToSeriesMap.containsKey(item.getTable())) {
                                tableToSeriesMap.get(item.getTable()).add(item.transToColumn());
                            }
                            else {
                                List<PrometheusColumn> items = new ArrayList<>();
                                items.add(item.transToColumn());
                                tableToSeriesMap.put(item.getTable(), items);
                            }
                        });

                        // table -> columns
                        tableToSeriesMap.entrySet().forEach(entry -> {
                            PrometheusTable t =
                                    new PrometheusTable(entry.getKey(), databaseName, entry.getValue(), null);
                            entry.getValue().forEach(column -> column.setTable(t));
                            databaseTables.add(t);
                        });
                    }
                }
                return new PrometheusSchema(databaseTables);
            } catch (Exception e) {
                ex = e;
            }
        }
        throw new AssertionError(ex);
    }

    public PrometheusSchema(List<PrometheusTable> databaseTables) {
        super(databaseTables);
    }

    public PrometheusTables getRandomTableNonEmptyTables() {
        return new PrometheusTables(Randomly.nonEmptySubset(getDatabaseTables()));
    }

}
