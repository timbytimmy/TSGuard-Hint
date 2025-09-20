package com.fuzzy.iotdb.gen;

import com.fuzzy.GlobalState;
import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.iotdb.IotDBGlobalState;
import com.fuzzy.iotdb.IotDBSchema.IotDBDataType;
import com.fuzzy.iotdb.IotDBSchema.IotDBTable.CompressionType;
import com.fuzzy.iotdb.ast.IotDBConstant;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class IotDBColumnGenerator {
    private final StringBuilder sb = new StringBuilder();
    private final String tableName;
    private final String columnName;
    private final IotDBGlobalState globalState;

    public IotDBColumnGenerator(IotDBGlobalState globalState, String tableName, String columnName) {
        this.columnName = columnName;
        this.tableName = tableName;
        this.globalState = globalState;
    }

    public static SQLQueryAdapter generate(IotDBGlobalState globalState, String tableName, String columnName) {
        return new IotDBColumnGenerator(globalState, tableName, columnName).create();
    }

    private SQLQueryAdapter create() {
        if (Randomly.getBoolean()) createTimeSeries();
        else createAlignedTimeSeries();

        // 创建Table
        return new SQLQueryAdapter(sb.substring(0, sb.length() - 1), new ExpectedErrors(), false);
    }

    private void createTimeSeries() {
        // 创建时间序列数据, 创建列
        sb.append("CREATE TIMESERIES ")
                .append(String.format("%s.%s.%s ", globalState.getDatabaseName(), tableName, columnName));

        IotDBDataType dataType = IotDBDataType.getRandom(globalState);
        if (Randomly.getBoolean()) {
            // 完整版
            sb.append("WITH ").append(String.format("datatype=%s ", dataType.getTypeName()));
        } else {
            // 简化版
            sb.append(String.format("%s ", dataType.getTypeName()));
        }

        appendEncodingType(dataType);
        appendCompressionType();
        if (IotDBDataType.FLOAT.equals(dataType) || IotDBDataType.DOUBLE.equals(dataType)) appendFloatPrecision();
        appendTags();
        appendAttributes();
    }

    private void createAlignedTimeSeries() {
        // 创建时间序列数据, 创建列
        sb.append("CREATE ALIGNED TIMESERIES ")
                .append(String.format("%s.aligned_%s(", globalState.getDatabaseName(), tableName));
        sb.append(columnName);
        IotDBDataType dataType = IotDBDataType.getRandom(globalState);
        sb.append(String.format(" %s ", dataType.getTypeName()));
        appendEncodingType(dataType);
        appendCompressionType();
        appendTags();
        appendAttributes();
        sb.deleteCharAt(sb.length() - 1).append(") ");
    }

    private void appendEncodingType(IotDBDataType dataType) {
        if (Randomly.getBoolean())
            sb.append(String.format("encoding=%s ", Randomly.fromList(dataType.getSupportedEncoding())));
    }

    private void appendCompressionType() {
        if (Randomly.getBoolean())
            sb.append(String.format("compressor=%s ", Randomly.fromList(Arrays.asList(CompressionType.values()))));
    }

    private void appendFloatPrecision() {
        sb.append(String.format("'MAX_POINT_NUMBER'='%d' ", IotDBConstant.IotDBDoubleConstant.scale));
    }

    private void appendTags() {
        if (Randomly.getBoolean()) {
            Map<String, String> tags = genStringMap(globalState);
            sb.append(String.format("tags(%s) ", tagsOrAttributesFlattening(tags)));
        }
    }

    private void appendAttributes() {
        if (Randomly.getBoolean()) {
            Map<String, String> attributes = genStringMap(globalState);
            sb.append(String.format("attributes(%s) ", tagsOrAttributesFlattening(attributes)));
        }
    }

    private Map<String, String> genStringMap(GlobalState globalState) {
        Map<String, String> map = new HashMap<>();
        for (int i = 0; i < 1 + Randomly.smallNumber(); i++) {
            String key = globalState.getRandomly().getNotBlankString();
            String value = globalState.getRandomly().getNotBlankString();
            if (!map.containsKey(key)) map.put(key, value);
        }
        return map;
    }

    private String tagsOrAttributesFlattening(Map<String, String> map) {
        StringBuilder sb = new StringBuilder();
        map.entrySet().forEach(entry -> {
            sb.append(entry.getKey()).append("=").append(entry.getValue()).append(",");
        });
        return sb.substring(0, sb.length() - 1);
    }

}
