package com.benchmark.commonUtil;

public class SpentTimeCalculator {
    private static SpentTimeCalculator defaultSpentTimeCalculator = new SpentTimeCalculator();
    private long startTime;
    private long endTime;

    public SpentTimeCalculator() {
    }

    public static SpentTimeCalculator create() {
        return new SpentTimeCalculator();
    }

    public static SpentTimeCalculator defaultSpentTimeCalculator() {
        return defaultSpentTimeCalculator;
    }

    public SpentTimeCalculator begin() {
        this.startTime = System.currentTimeMillis();
        return this;
    }

    public SpentTimeCalculator end() {
        this.endTime = System.currentTimeMillis();
        return this;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getSpendTime() {
        return this.endTime - this.startTime;
    }

    @Override
    public String toString() {
        return "TimeUtil{" +
                "spend=" + this.getSpendTime() + "(ms)" +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                '}';
    }
}
