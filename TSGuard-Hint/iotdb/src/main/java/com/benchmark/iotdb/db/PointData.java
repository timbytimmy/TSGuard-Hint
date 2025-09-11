package com.benchmark.iotdb.db;

import com.benchmark.commonUtil.CommonUtil;
import com.benchmark.constants.ValueStatus;
import lombok.Builder;
import lombok.Data;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.Serializable;

@Builder
@Data
public class PointData implements Serializable {
    private static final long serialVersionUID = 1L;
    private ValueStatus valueStatus = ValueStatus.VALID;

    private String tableName;        // metrics，如driverLocation
    private String tagName;            // tagName，如driverId
    private String tagValue;        // tagValue，如10051
    private long utcTime;            // 数值型实时UTC时间

    private String fieldName;        // 数值型点名
    private TSDataType fieldType;    // 数据类型
    private Object fieldValue;        // 数据值

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

