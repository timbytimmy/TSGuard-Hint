package com.fuzzy.common.tsaf;

public enum QueryType {
    BASE_QUERY,
    TIME_WINDOW_QUERY,
    TIME_SERIES_FUNCTION;

    public boolean isBaseQuery() {
        return this.equals(QueryType.BASE_QUERY);
    }

    public boolean isTimeWindowQuery() {
        return this.equals(QueryType.TIME_WINDOW_QUERY);
    }

    public boolean isTimeSeriesFunction() {
        return this.equals(QueryType.TIME_SERIES_FUNCTION);
    }
}
