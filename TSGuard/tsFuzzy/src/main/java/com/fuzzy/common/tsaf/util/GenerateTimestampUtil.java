package com.fuzzy.common.tsaf.util;

import java.util.ArrayList;
import java.util.List;

public class GenerateTimestampUtil {
    public static List<Long> genTimestampsInRange(Long startTime, Long endTime, Long samplingFrequency) {
        // 指定时间范围内进行时间戳采样, 采取左闭右闭(存在 time <= constant 的情况)
        List<Long> timestamps = new ArrayList<>();
        for (long i = startTime; i <= endTime; i += samplingFrequency) timestamps.add(i);
        return timestamps;
    }
}
