package com.benchmark.dto;

import com.benchmark.constants.ValueStatus;
import lombok.Builder;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Jacksonized
@Builder
public class DBValParam {
    private static final long serialVersionUID = 1L;

    // TODO 参数非空注解
    private String tableName;    // metrics，如driverLocation
    private String tagName;        // tagName，如driverId
    private String tagValue;    // tagValue，如10051
    private ValueStatus valueStatus;        // 数值型实时状态

    private String fieldName;   // 查询字段名，为空则查询所有字段

    @Override
    public String toString() {
        if (!isValueValid()) {
            return "该Value无效！";
        }

        String str = String.format("%s,%s=%s ", this.tableName, this.tagName, this.tagValue);

        if (fieldName != null) {
            str += String.format(" %s", this.fieldName);
        }
        return str;
    }

    // Getter
    public boolean isValueValid() {
        return ValueStatus.VALID == this.valueStatus;
    }

    // Setter
    public void setValueStatusInvalid() {
        this.valueStatus = ValueStatus.INVALID;
    }
}
