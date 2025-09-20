package com.benchmark.fieldName;

import com.benchmark.constants.DataType;
import com.benchmark.fieldName.instance.DriverLocationFieldName;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class FieldName {
    //TODO 需加入若干tags

    private String tagName;             // tagName，如driverId
    private String timeStamp;           // 时间戳 - 均以字符串形式显示
    private DataType timeStampType;     // 时间戳格式 - Long or String

    private List<String> names;         // 若干字段及类型
    private List<DataType> types;
    private int listSize;

    // 若干子类 - 支持动态扩展实例对象
    public FieldName(DriverLocationFieldName[] driverLocationFieldNames) {
        this.names = new ArrayList<>();
        this.types = new ArrayList<>();
        this.listSize = 0;

        // 将各JSON数据类型fieldName静态对象转存至通用类FieldName中
        for (DriverLocationFieldName driverLocationFieldName : driverLocationFieldNames) {
            if (driverLocationFieldName == DriverLocationFieldName.TAG) {
                this.tagName = driverLocationFieldName.getName();
            } else if (driverLocationFieldName == DriverLocationFieldName.TIMESTAMP) {
                this.timeStamp = driverLocationFieldName.getName();
                this.timeStampType = driverLocationFieldName.getType();
            } else {
                this.addField(driverLocationFieldName.getName(), driverLocationFieldName.getType());
            }
        }
    }

    private void addField(String fieldName, DataType fieldType) {
        this.names.add(fieldName);
        this.types.add(fieldType);
        this.listSize++;
    }

    // 私有化，仅可通过addField添加字段组合
    private void setNames(List<String> names) {
        this.names = names;
    }

    private void setTypes(List<DataType> types) {
        this.types = types;
    }

    private void setListSize(int listSize) {
        this.listSize = listSize;
    }
}
