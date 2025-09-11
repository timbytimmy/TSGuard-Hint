package com.benchmark.entity;

import com.benchmark.commonUtil.CommonUtil;
import com.benchmark.constants.DataType;
import com.benchmark.constants.ValueStatus;
import lombok.Data;

import java.io.Serializable;

// 单Value数据点
@Data
public class PointData implements Serializable {
    private static final long serialVersionUID = 1L;

    private String tableName;        // metrics，如driverLocation
    private String tagName;            // tagName，如driverId
    private String tagValue;        // tagValue，如10051
    private long utcTime;            // 数值型实时UTC时间
    private ValueStatus valueStatus = ValueStatus.VALID;

    private String fieldName;        // 数值型点名
    private DataType fieldType;        // 数据类型
    private Object fieldValue;        // 数据值

    // 建造者
    public static final class PointDataBuilder {
        private static final long serialVersionUID = 1L;

        private String tableName;
        private String tagName;
        private String tagValue;
        private long utcTime;
        private ValueStatus valueStatus = ValueStatus.VALID;

        private String fieldName;
        private DataType fieldType;
        private Object fieldValue;

        private void init() {
            this.tableName = "";
            // 针对某pointdata，不传参默认为Valid
            this.valueStatus = ValueStatus.VALID;
        }

        private PointDataBuilder() {
            this.init();
        }

        public static PointDataBuilder anPointData() {
            return new PointDataBuilder();
        }

        public static PointDataBuilder anInValidPointData() {
            return new PointDataBuilder().withValueStatus(ValueStatus.INVALID);
        }

        public static PointDataBuilder anCommonPointData(PointData pointData) {
            return anPointData()
                    .withTableName(pointData.getTableName())
                    .withTagName(pointData.getTagName())
                    .withTagValue(pointData.getTagValue())
                    .withUtcTime(pointData.getUtcTimeMilliSeconds())
                    .withValueStatus(pointData.getValueStatus())
                    .withFieldName(pointData.getFieldName())
                    .withFieldValue(pointData.getFieldValue())
                    .withFieldType(pointData.getFieldType());
        }

        public PointDataBuilder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public PointDataBuilder withTagName(String tagName) {
            this.tagName = tagName;
            return this;
        }

        public PointDataBuilder withTagValue(String tagValue) {
            this.tagValue = tagValue;
            return this;
        }

        public PointDataBuilder withUtcTime(long utcTime) {
            this.utcTime = utcTime;
            return this;
        }

        public PointDataBuilder withValueStatus(ValueStatus valueStatus) {
            this.valueStatus = valueStatus;
            return this;
        }

        public PointDataBuilder withFieldName(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public PointDataBuilder withFieldValue(Object fieldValue) {
            this.fieldValue = fieldValue;
            return this;
        }

        public PointDataBuilder withFieldType(DataType fieldType) {
            this.fieldType = fieldType;
            return this;
        }

        public PointData build() {
            PointData pointData = new PointData();
            pointData.setTableName(tableName);
            pointData.setTagName(tagName);
            pointData.setTagValue(tagValue);
            pointData.setUtcTimeMilliSeconds(utcTime);
            pointData.setValueStatus(valueStatus);

            pointData.setFieldName(fieldName);
            pointData.setFieldValue(fieldValue);
            pointData.setFieldType(fieldType);

            return pointData;
        }
    }

    protected PointData(String fieldName, String tagName, String tagValue, ValueStatus status) {
        this.tableName = "";
        this.tagName = tagName;
        this.tagValue = tagValue;
        this.valueStatus = status;
        this.fieldName = fieldName;
    }

    protected PointData() {
        this.setValueStatusInvalid();
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
     * utcTime 单位为秒
     */
    public void setUtcTime(long utcTime) {
        this.utcTime = utcTime * 1000;
    }

    @Override
    public String toString() {
        if (!isValueValid()) {
            return "该Value无效！";
        }

        String str = String.format("%s,%s=%s ", this.tableName, this.getTagName(), this.tagValue);
        str += String.format("%s=%s timestamp=%s,data=%s", this.fieldName, this.fieldValue.toString(),
                this.utcTime, CommonUtil.uTCMilliSecondsToDateStringWithMs(this.utcTime));

        return str;
    }

    public void setValueStatusInvalid() {
        this.valueStatus = ValueStatus.INVALID;
    }

    public boolean isValueValid() {
        return ValueStatus.VALID == this.valueStatus;
    }

    public String getTagName() {
        if (this.tagName == null || this.tagName.isEmpty()) {
            return "tagName";
        } else {
            return this.tagName;
        }
    }
}

