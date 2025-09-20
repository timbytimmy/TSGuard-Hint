package com.benchmark.entity;

import lombok.Builder;
import lombok.Data;

@Builder
@Data
public class PerformanceEntity {

    private long timeCost;          // 执行开销
    private boolean isSuccess;      // 操作是否成功
    private Object obj;             // 结果实体

}
