package com.benchmark.iotdb.db;

import com.benchmark.constants.ValueStatus;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.util.List;

/**
 * @description 单个record，指一个设备一个时间戳下多个测点的数据。相当于一个measurementname，对应多个filed
 * @dateTime 2022/11/29 22:56
 */
@NoArgsConstructor
@Data
public class Record {
    private ValueStatus valueStatus = ValueStatus.VALID;        //该pointData是否有效

    private String prefixPath;          // 度量名+tags
    private List<String> measurements;  // fields：风力、温度、湿度
    private List<TSDataType> types;     // valueType
    private List<Object> values;
    private long timestamp;

    public Record(String prefixPath, List<String> measurements, List<TSDataType> types,
                  List<Object> values, long timestamp) {
        this.prefixPath = prefixPath;
        this.measurements = measurements;
        this.types = types;
        this.values = values;
        this.timestamp = timestamp;
    }

    public void setValueStatusInvalid() {
        this.valueStatus = ValueStatus.INVALID;
    }

    public boolean isValueValid() {
        return ValueStatus.VALID == this.valueStatus;
    }
}
