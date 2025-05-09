package com.benchmark.entity;

import com.benchmark.constants.ValueStatus;
import lombok.Data;
import lombok.Getter;

// 数目聚合结果
@Getter
@Data
public class AggCountResult {

    private String tableName;        // metrics，如driverLocation
    private String tagName;            // tagName，如driverId
    private String tagValue;        // tagValue，如10051
    private Long count;          // 数目聚合

    private ValueStatus valueStatus;

    public static final class AggCountResultBuilder {
        private String tableName;        // metrics，如driverLocation
        private String tagName;            // tagName，如driverId
        private String tagValue;        // tagValue，如10051
        private Long count;          // 数目聚合
        private ValueStatus valueStatus;

        private void init() {
            this.tableName = "";
            this.valueStatus = ValueStatus.VALID;
        }

        private AggCountResultBuilder() {
            this.init();
        }

        public static AggCountResult.AggCountResultBuilder anResult() {
            return new AggCountResult.AggCountResultBuilder();
        }

        public AggCountResult.AggCountResultBuilder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public AggCountResult.AggCountResultBuilder withTagName(String tagName) {
            this.tagName = tagName;
            return this;
        }

        public AggCountResult.AggCountResultBuilder withTagValue(String tagValue) {
            this.tagValue = tagValue;
            return this;
        }

        public AggCountResult.AggCountResultBuilder withCount(Long count) {
            this.count = count;
            return this;
        }

        public AggCountResult.AggCountResultBuilder setResultInvalid() {
            this.valueStatus = ValueStatus.INVALID;
            return this;
        }

        public AggCountResult build() {
            AggCountResult result = new AggCountResult();
            result.setTableName(tableName);
            result.setTagName(tagName);
            result.setTagValue(tagValue);
            result.setCount(count);
            result.setValueStatus(valueStatus);

            return result;
        }
    }

    @Override
    public String toString() {
        String str = String.format("%s,%s=%s ", this.tableName, this.getTagName(), this.tagValue);
        str += String.format("count=%s", this.count);

        return str;
    }

    public boolean isValueValid() {
        return ValueStatus.VALID == this.valueStatus;
    }
}
