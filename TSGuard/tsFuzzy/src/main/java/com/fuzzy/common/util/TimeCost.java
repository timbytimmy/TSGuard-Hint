package com.fuzzy.common.util;

/**
 * 功能描述
 *
 * @author: scott
 * @date: 2022年05月02日 1:01 AM
 */
public class TimeCost {
    long begin;
    long end;

    public TimeCost begin() {
        this.begin = System.currentTimeMillis();
        return this;
    }

    public TimeCost end() {
        this.end = System.currentTimeMillis();
        return this;
    }

    public long getBegin() {
        return begin;
    }
    public long getEnd() {
        return end;
    }
    public long getCost() {
        return this.end-this.begin;
    }
}
