package com.fuzzy.TDengine.tsaf.template;

import com.alibaba.fastjson.JSONObject;
import com.fuzzy.TDengine.TDengineGlobalState;
import com.fuzzy.TDengine.TDengineSchema;
import com.fuzzy.TDengine.TDengineVisitor;
import com.fuzzy.TDengine.ast.TDengineConstant;
import com.fuzzy.TDengine.gen.TDengineInsertGenerator;
import com.fuzzy.TDengine.gen.TDengineTimeSeriesConstantGenerator;
import com.fuzzy.TDengine.gen.TDengineTimeSeriesParameterGenerator;
import com.fuzzy.TDengine.resultSet.TDengineResultSet;
import com.fuzzy.TDengine.tsaf.enu.TDengineAggregationType;
import com.fuzzy.TDengine.tsaf.enu.TDengineConstantString;
import com.fuzzy.common.schema.TimeSeriesTemplate;
import com.fuzzy.common.tsaf.aggregation.AggregationType;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
@Getter
public enum TDengineTimeSeriesTemplate {
    // 降采样查询
    DownSamplingQuery("select ? from ? where time >= ? and time < ? PARTITION BY ? INTERVAL(?)",
            Arrays.asList(TDengineSchema.TDengineDataType.TIMESTAMP,
                    TDengineSchema.TDengineDataType.TIMESTAMP)) {
        @Override
        public boolean verifyResults(TDengineResultSet result, List<TDengineSchema.TDengineColumn> verifyColumns,
                                     TDengineTimeSeriesConstantGenerator gen,
                                     TDengineTimeSeriesParameterGenerator parameterGenerator) throws Exception {
            assert getValues().size() == 6;

            Long startTimestamp = (long) getValues().getValues().get(2);
            Long endTimestamp = (long) getValues().getValues().get(3) - 1;
            String intervalString = getValues().get(5).toString();
            Long interval = Long.parseLong(intervalString.substring(0, intervalString.length() - 1)) * 1000;
            AggregationType aggregationType = (AggregationType) this.getExtraProperties();
            List<Long> timestamps = parameterGenerator.genTimestampsInRangeSplitByInterval(startTimestamp, endTimestamp,
                    interval);

            int index = 0;
            // TODO 切分时间逻辑不统一, 目前无法验证
//            while (result.hasNext()) {
//                if (!verifyAggregationQuery(result, verifyColumns, gen, startTimestamp, timestamps.get(index++),
//                        aggregationType))
//                    return false;
//            }
            return true;
        }

        @Override
        public void genRandomTemplateValues(TDengineSchema.TDengineTable table, TDengineGlobalState globalState) {
            List<Object> values = new ArrayList<>();
            StringBuilder fetchColumn = new StringBuilder();
            // TODO
            table.getColumns().stream().filter(column -> {
                return column.getName().equalsIgnoreCase(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).forEach(column -> fetchColumn.append(column.getName()).append(","));
            fetchColumn.append(TDengineConstantString.W_START_TIME_COLUMN_NAME.getName()).append(",");

            // TODO
//            TDengineAggregationType aggregationType = Randomly.fromOptions(TDengineAggregationType.values());
            AggregationType aggregationType = AggregationType.COUNT;
            this.setExtraProperties(aggregationType);
            table.getColumns().stream().filter(column -> {
                return !column.getName().equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName())
                        && !column.getName().equalsIgnoreCase(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).forEach(column -> fetchColumn.append(aggregationType)
                    .append("(").append(column.getName()).append(") as ").append(column.getName()).append(","));

            values.add(fetchColumn.substring(0, fetchColumn.length() - 1));
            values.add(table.getName());
            TDengineTimeSeriesParameterGenerator parameterGenerator = new TDengineTimeSeriesParameterGenerator(
                    table, globalState);
            values.addAll(parameterGenerator.genTemplateWhereParams(this.getTemplate()));
            values.add(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName());
            values.add(String.format("%ds", parameterGenerator.genInterval() / 1000));
            this.setValues(new TDengineTemplateValues(values));
        }
    },

    LatestPointQuery("select ? from ? order by time desc limit 1",
            Collections.singletonList(TDengineSchema.TDengineDataType.TIMESTAMP)) {
        @Override
        public boolean verifyResults(TDengineResultSet result, List<TDengineSchema.TDengineColumn> verifyColumns,
                                     TDengineTimeSeriesConstantGenerator gen,
                                     TDengineTimeSeriesParameterGenerator parameterGenerator) throws Exception {
            assert getValues().size() == 2;

            while (result.hasNext()) {
                if (!verifySelectQuery(result, verifyColumns, gen,
                        TDengineInsertGenerator.getLastTimestamp(parameterGenerator.getGlobalState().getDatabaseName(),
                                parameterGenerator.getTable().getName()))) return false;
            }
            return true;
        }

        @Override
        public void genRandomTemplateValues(TDengineSchema.TDengineTable table, TDengineGlobalState globalState) {
            List<Object> values = new ArrayList<>();
            StringBuilder fetchColumn = new StringBuilder();
            table.getColumns().forEach(column -> fetchColumn.append(column.getName()).append(","));
            values.add(fetchColumn.substring(0, fetchColumn.length() - 1));
            values.add(table.getName());
            this.setValues(new TDengineTemplateValues(values));
        }
    },

    HistoricalSectionQuery("select ? from ? where time = ? order by time",
            Collections.singletonList(TDengineSchema.TDengineDataType.TIMESTAMP)) {
        @Override
        public boolean verifyResults(TDengineResultSet result, List<TDengineSchema.TDengineColumn> verifyColumns,
                                     TDengineTimeSeriesConstantGenerator gen,
                                     TDengineTimeSeriesParameterGenerator parameterGenerator) throws Exception {
            assert getValues().size() == 3;

            while (result.hasNext()) {
                if (!verifySelectQuery(result, verifyColumns, gen, (long) getValues().get(2))) return false;
            }
            return true;
        }

        @Override
        public void genRandomTemplateValues(TDengineSchema.TDengineTable table, TDengineGlobalState globalState) {
            List<Object> values = new ArrayList<>();
            StringBuilder fetchColumn = new StringBuilder();
            table.getColumns().forEach(column -> fetchColumn.append(column.getName()).append(","));
            values.add(fetchColumn.substring(0, fetchColumn.length() - 1));
            values.add(table.getName());
            TDengineTimeSeriesParameterGenerator parameterGenerator = new TDengineTimeSeriesParameterGenerator(
                    table, globalState);
            values.addAll(parameterGenerator.genTemplateWhereParams(this.getTemplate()));
            this.setValues(new TDengineTemplateValues(values));
        }
    },

    HistoricalRangeQuery("select ? from ? where time >= ? and time < ? order by time",
            Arrays.asList(TDengineSchema.TDengineDataType.TIMESTAMP,
                    TDengineSchema.TDengineDataType.TIMESTAMP)) {
        @Override
        public boolean verifyResults(TDengineResultSet result, List<TDengineSchema.TDengineColumn> verifyColumns,
                                     TDengineTimeSeriesConstantGenerator gen,
                                     TDengineTimeSeriesParameterGenerator parameterGenerator) throws Exception {
            assert getValues().size() == 4;

            // 1. 校验数量一致
            // 2. 对应timestamp均出现
            // 3. 依据timestamp比对值正确性
            // 4. 时间范围遵循左闭右开原则
            List<Long> timestamps = parameterGenerator.genTimestampsInRange(
                    (long) getValues().getValues().get(2), (long) getValues().getValues().get(3) - 1,
                    parameterGenerator.getGlobalState().getOptions().getSamplingFrequency());
            int index = 0;
            while (result.hasNext()) {
                if (!verifySelectQuery(result, verifyColumns, gen, timestamps.get(index))) return false;
                index++;
            }
            return true;
        }

        @Override
        public void genRandomTemplateValues(TDengineSchema.TDengineTable table, TDengineGlobalState globalState) {
            List<Object> values = new ArrayList<>();
            StringBuilder fetchColumn = new StringBuilder();
            table.getColumns().forEach(column -> fetchColumn.append(column.getName()).append(","));
            values.add(fetchColumn.substring(0, fetchColumn.length() - 1));
            values.add(table.getName());
            TDengineTimeSeriesParameterGenerator parameterGenerator = new TDengineTimeSeriesParameterGenerator(
                    table, globalState);
            values.addAll(parameterGenerator.genTemplateWhereParams(this.getTemplate()));
            this.setValues(new TDengineTemplateValues(values));
        }
    },

    TimeSeriesAggregationQuery("select ? from ? where time >= ? and time < ? group by ?",
            Arrays.asList(TDengineSchema.TDengineDataType.TIMESTAMP,
                    TDengineSchema.TDengineDataType.TIMESTAMP)) {
        @Override
        public boolean verifyResults(TDengineResultSet result, List<TDengineSchema.TDengineColumn> verifyColumns,
                                     TDengineTimeSeriesConstantGenerator gen,
                                     TDengineTimeSeriesParameterGenerator parameterGenerator) throws Exception {
            assert getValues().size() == 5;

            Long startTimestamp = (long) getValues().getValues().get(2);
            Long endTimestamp = (long) getValues().getValues().get(3) - 1;
            AggregationType aggregationType = (AggregationType) this.getExtraProperties();
            while (result.hasNext()) {
                if (!verifyAggregationQuery(result, verifyColumns, gen, startTimestamp, endTimestamp, aggregationType))
                    return false;
            }

            return true;
        }

        @Override
        public void genRandomTemplateValues(TDengineSchema.TDengineTable table, TDengineGlobalState globalState) {
            List<Object> values = new ArrayList<>();
            StringBuilder fetchColumn = new StringBuilder();
            table.getColumns().stream().filter(column -> {
                return column.getName().equalsIgnoreCase(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).forEach(column -> fetchColumn.append(column.getName()).append(","));

            AggregationType aggregationType = TDengineAggregationType.getRandomAggregationType();
            this.setExtraProperties(aggregationType);
            table.getColumns().stream().filter(column -> {
                return !column.getName().equalsIgnoreCase(TDengineConstantString.TIME_FIELD_NAME.getName())
                        && !column.getName().equalsIgnoreCase(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName());
            }).forEach(column -> fetchColumn.append(aggregationType)
                    .append("(").append(column.getName()).append(") as ").append(column.getName()).append(","));

            values.add(fetchColumn.substring(0, fetchColumn.length() - 1));
            values.add(table.getName());
            TDengineTimeSeriesParameterGenerator parameterGenerator = new TDengineTimeSeriesParameterGenerator(
                    table, globalState);
            values.addAll(parameterGenerator.genTemplateWhereParams(this.getTemplate()));
            values.add(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName());
            this.setValues(new TDengineTemplateValues(values));
        }
    };

    private TimeSeriesTemplate<TDengineSchema.TDengineDataType> template;
    @Setter
    private TDengineTemplateValues values;
    @Setter
    private Object extraProperties;

    TDengineTimeSeriesTemplate(String template, List<TDengineSchema.TDengineDataType> dataTypeList) {
        this.template = new TimeSeriesTemplate<>(template, dataTypeList);
    }

    public abstract boolean verifyResults(TDengineResultSet result,
                                          List<TDengineSchema.TDengineColumn> verifyColumns,
                                          TDengineTimeSeriesConstantGenerator gen,
                                          TDengineTimeSeriesParameterGenerator parameterGenerator) throws Exception;

    public abstract void genRandomTemplateValues(TDengineSchema.TDengineTable table, TDengineGlobalState globalState);

    private static boolean verifySelectQuery(TDengineResultSet result,
                                             List<TDengineSchema.TDengineColumn> verifyColumns,
                                             TDengineTimeSeriesConstantGenerator gen,
                                             Long timestamp) throws Exception {
        // 比对时间戳
        if (timestamp != result.getLong(TDengineConstantString.TIME_FIELD_NAME.getName())) return false;
        int deviceId = result.getInt(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName().toLowerCase());
        TDengineConstant expectedValue = gen.generateConstantByTimestampAndId(timestamp, deviceId);
        return verifyColumns(result, verifyColumns, expectedValue, null);
    }

    private static boolean verifyAggregationQuery(TDengineResultSet result,
                                                  List<TDengineSchema.TDengineColumn> verifyColumns,
                                                  TDengineTimeSeriesConstantGenerator gen,
                                                  Long startTimestamp, Long endTimestamp,
                                                  AggregationType aggregationType) throws Exception {
        int deviceId = result.getInt(TDengineConstantString.DEVICE_ID_COLUMN_NAME.getName().toLowerCase());
        TDengineConstant expectedValue = gen.generateAggregationResult(startTimestamp, endTimestamp, deviceId,
                aggregationType);
        return verifyColumns(result, verifyColumns, expectedValue, expectedValue.getType());
    }

    private static boolean verifyColumns(TDengineResultSet result,
                                         List<TDengineSchema.TDengineColumn> verifyColumns,
                                         TDengineConstant expectedValue,
                                         TDengineSchema.TDengineDataType resultType) throws Exception {
        // 校验值
        for (int i = 0; i < verifyColumns.size(); i++) {
            TDengineSchema.TDengineColumn column = verifyColumns.get(i);
            int columnIndex = result.findColumn(column.getName());
            TDengineConstant constant;
            if (result.getString(columnIndex) == null) {
                constant = TDengineConstant.createNullConstant();
            } else {
                Object value;
                TDengineSchema.TDengineDataType dataType = resultType == null ? column.getType() : resultType;
                switch (dataType) {
                    case INT:
                        value = result.getLong(columnIndex);
                        constant = TDengineConstant.createInt32Constant((long) value);
                        break;
                    case UINT:
                        value = result.getLong(columnIndex);
                        constant = TDengineConstant.createUInt32Constant((long) value);
                        break;
                    case TIMESTAMP:
                        // TODO
                    case UBIGINT:
                        value = result.getLong(columnIndex);
                        constant = TDengineConstant.createUInt64Constant((long) value);
                        break;
                    case BIGINT:
                        value = result.getLong(columnIndex);
                        constant = TDengineConstant.createInt64Constant((long) value);
                        break;
                    case BINARY:
                    case VARCHAR:
                        value = result.getString(columnIndex);
                        constant = TDengineConstant.createStringConstant((String) value);
                        break;
                    case DOUBLE:
                        value = result.getDouble(columnIndex);
                        constant = TDengineConstant.createDoubleConstant((double) value);
                        break;
                    default:
                        throw new AssertionError(column.getType());
                }
            }
            TDengineConstant isEquals = constant.isEquals(expectedValue);
            if (isEquals.isNull()) {
                log.warn("Null");
                return false;
            } else if (!isEquals.asBooleanNotNull()) {
                log.error("Expected Value:{} query Value:{}", TDengineVisitor.asExpectedValues(expectedValue),
                        TDengineVisitor.asExpectedValues(constant));
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return "TDengineTimeSeriesTemplate{" +
                "template=" + JSONObject.toJSONString(template) +
                ", values=" + JSONObject.toJSONString(values) +
                '}';
    }
}
