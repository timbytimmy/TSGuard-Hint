package com.fuzzy;

import com.fuzzy.common.query.Query;
import com.fuzzy.common.schema.AbstractSchema;
import com.fuzzy.util.ExecutionTimer;

import java.time.Instant;
import java.util.Date;

/**
 * Represents a global state that is valid for a testing session on a given database.
 *
 * @param <O> the option parameter
 * @param <S> the schema parameter
 */
public abstract class SQLGlobalState<O extends DBMSSpecificOptions<?>, S extends AbstractSchema<?, ?>>
        extends GlobalState<O, S, SQLConnection> {

    @Override
    protected void executeEpilogue(Query<?> q, boolean success, ExecutionTimer timer) throws Exception {
        boolean logExecutionTime = getOptions().logExecutionTime();
        if (success && getOptions().printSucceedingStatements()) {
            System.out.println(q.getQueryString());
        }
        if (logExecutionTime) {
            getLogger().writeCurrent(" -- " + timer.end().asString());
        }
        if (q.couldAffectSchema()) {
            updateSchema();
        }
    }

    public long getNextSampleTimestamp(long lastTimestamp) {
        return lastTimestamp;
    }

    public long getRandomTimestamp() {
        // 依据精度返回时间戳
        long timestamp = this.getRandomly().getLong(transTimestampToMS(this.getOptions().getStartTimestampOfTSData()),
                new Date().getTime());
        return transTimestampToPrecision(timestamp);
    }

    // date格式(ISO 8601): "2022-01-01T08:00:00Z"
    public long transDateToTimestamp(String date) {
        Instant instant = Instant.parse(date);
        long timestamp = instant.toEpochMilli();
        return transTimestampToPrecision(timestamp);
    }

    public long transTimestampToPrecision(long timestamp) {
        // timestamp默认ms级别
        long res = timestamp;
        switch (getOptions().getPrecision()) {
            case "us":
                res *= 1000;
                break;
            case "ns":
                res *= 1000 * 1000;
                break;
            case "s":
                res /= 1000;
                break;
            case "ms":
            default:
        }
        return res;
    }

    public long transTimestampToMS(long timestamp) {
        long res = timestamp;
        switch (getOptions().getPrecision()) {
            case "us":
                res /= 1000;
                break;
            case "ns":
                res /= 1000 * 1000;
                break;
            case "ms":
                break;
            case "s":
                res *= 1000;
                break;
            default:
        }
        return res;
    }

    public long transTimestampFromNSToPrecision(long timestamp, String precision) {
        long res = timestamp;
        switch (precision) {
            case "us":
                res /= 1000;
                break;
            case "ms":
                res /= 1000 * 1000;
                break;
            case "s":
                res /= 1000 * 1000 * 1000;
                break;
            case "ns":
            default:
        }
        return res;
    }
}
