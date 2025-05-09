package com.benchmark.entity;

import com.benchmark.constants.DataType;
import com.benchmark.constants.ValueStatus;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class DBVal implements Serializable {
    private static final long serialVersionUID = 1L;

    private String tableName;    // metrics，如driverLocation
    private String tagName;        // tagName，如driverId
    private String tagValue;    // tagValue，如10051
    private long utcTime;       // 数值型实时UTC时间，精确到ms
    private ValueStatus valueStatus = ValueStatus.VALID;        // 数值型实时状态

    private List<String> fieldNames;    // 字段名，如speed、lng等
    private List<Object> fieldValues;    // 数值
    private List<DataType> fieldTypes;    // 数值类型
    private int fieldSize;              // fields数目

    // 建造者
    public static final class DBValBuilder {
        private static final long serialVersionUID = 1L;

        private String tableName;    // metrics，如driverLocation
        private String tagName;        // tagName，如driverId
        private String tagValue;    // tagValue，如10051
        private long utcTime;        // 数值型实时UTC时间
        private ValueStatus valueStatus = ValueStatus.VALID;        // 数值型实时状态

        private List<String> fieldNames;    // 字段名，如speed、lng等
        private List<Object> fieldValues;    // 数值
        private List<DataType> fieldTypes;    // 数值类型
        private int fieldSize;              // fields数目

        private void init() {
            this.valueStatus = ValueStatus.VALID;
            this.fieldNames = new ArrayList<>();
            this.fieldValues = new ArrayList<>();
            this.fieldTypes = new ArrayList<>();
            this.fieldSize = 0;
        }

        private DBValBuilder() {
            this.init();
        }

        public static DBVal.DBValBuilder anDBVal() {
            return new DBVal.DBValBuilder();
        }

        public static DBValBuilder anCommonDBVal(DBVal dbVal) {
            return anDBVal()
                    .withTableName(dbVal.getTableName())
                    .withTagName(dbVal.getTagName())
                    .withTagValue(dbVal.getTagValue())
                    .withUtcTime(dbVal.getUtcTimeMilliSeconds())
                    .withValueStatus(dbVal.getValueStatus())
                    .withFieldNames(dbVal.getFieldNames())
                    .withFieldValues(dbVal.getFieldValues())
                    .withFieldTypes(dbVal.getFieldTypes());
        }

        public DBVal.DBValBuilder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public DBVal.DBValBuilder withTagName(String tagName) {
            this.tagName = tagName;
            return this;
        }

        public DBVal.DBValBuilder withTagValue(String tagValue) {
            this.tagValue = tagValue;
            return this;
        }

        public DBVal.DBValBuilder withUtcTime(long utcTime) {
            this.utcTime = utcTime;
            return this;
        }

        public DBVal.DBValBuilder withValueStatus(ValueStatus valueStatus) {
            this.valueStatus = valueStatus;
            return this;
        }

        public DBVal.DBValBuilder withFieldNames(List<String> fieldNames) {
            this.fieldNames = fieldNames;
            return this;
        }

        public DBVal.DBValBuilder withFieldValues(List<Object> fieldValues) {
            this.fieldValues = fieldValues;
            return this;
        }

        public DBVal.DBValBuilder withFieldTypes(List<DataType> fieldTypes) {
            this.fieldTypes = fieldTypes;
            return this;
        }

        public DBVal.DBValBuilder withFieldSize(int fieldSize) {
            this.fieldSize = fieldSize;
            return this;
        }

        public DBVal build() {
            DBVal dbVal = new DBVal();
            dbVal.setTableName(tableName);
            dbVal.setTagName(tagName);
            dbVal.setTagValue(tagValue);
            dbVal.setUtcTimeMilliSeconds(utcTime);
            dbVal.setValueStatus(valueStatus);

            dbVal.setFieldNames(fieldNames);
            dbVal.setFieldValues(fieldValues);
            dbVal.setFieldTypes(fieldTypes);
            dbVal.setFieldSize(fieldSize);

            return dbVal;
        }
    }

    private DBVal() {
    }

    /**
     * @param fieldName
     * @param dataType
     * @param value
     * @return void
     * @description 逐个添加字段
     * @dateTime 2023/2/4 16:29
     */
    public void addField(String fieldName, DataType dataType, Object value) {
        this.fieldNames.add(fieldName);
        this.fieldTypes.add(dataType);
        this.fieldValues.add(value);
        this.fieldSize += 1;
    }

    /*
     * 返回时间戳，单位为秒
     */
    public long getUtcTime() {
        return utcTime / 1000;
    }

    /*
     * 返回时间戳，单位为毫秒
     */
    public long getUtcTimeMilliSeconds() {
        return utcTime;
    }

    /*
     * 返回时间戳，单位为毫秒
     */
    public void setUtcTimeMilliSeconds(long utcTime) {
        this.utcTime = utcTime;
    }

    /*
     * utcTime 单位为ms
     */
    public void setUtcTime(long utcTime) {
        this.utcTime = utcTime * 1000;
    }


    @Override
    public String toString() {
        if (!isValueValid()) {
            return "该Value无效！";
        }

        StringBuilder sb = new StringBuilder(String.format("%s,%s=%s ", this.tableName, this.tagName, this.tagValue));

        for (int i = 0; i < fieldSize; i++) {
            String fieldName = this.fieldNames.get(i);
            Object fieldValue = this.fieldValues.get(i);
            DataType fieldType = this.fieldTypes.get(i);

            // 根据给定数据格式转换
            switch (fieldType) {
                case INT32:
                case INT64:
                    // TODO 若JSON文件带小数点的字符串，无法解析为INT或LONG，目前暂将其通过Double接收，强转Long; 需修改
                    sb.append(String.format("%s=%d,", fieldName, (long) Double.parseDouble(fieldValue.toString())));
                    break;
                case FLOAT:
                    sb.append(String.format("%s=%.6f,", fieldName, Float.parseFloat(fieldValue.toString())));
                    break;
                case BOOLEAN:
                    sb.append(Boolean.parseBoolean(fieldValue.toString()) ? String.format("%s=%s,", fieldName, "true") :
                            String.format("%s=%s,", fieldName, "false"));
                    break;
                case DOUBLE:
                    sb.append(String.format("%s=%.9f,", fieldName, Double.parseDouble(fieldValue.toString())));
                    break;
                default:
            }
        }

        sb.deleteCharAt(sb.length()-1);
        sb.append(String.format(" %s", this.utcTime));
        return sb.toString();
    }

    public boolean isValueValid() {
        return ValueStatus.VALID == this.valueStatus;
    }

    public void setValueStatusInvalid() {
        this.valueStatus = ValueStatus.INVALID;
    }

    // 针对列表操作私有化，外部仅可通过addField添加field字段
    private void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    private void setFieldValues(List<Object> fieldValues) {
        this.fieldValues = fieldValues;
    }

    private void setFieldTypes(List<DataType> fieldTypes) {
        this.fieldTypes = fieldTypes;
    }

    private void setFieldSize(int fieldSize) {
        this.fieldSize = fieldSize;
    }
}
