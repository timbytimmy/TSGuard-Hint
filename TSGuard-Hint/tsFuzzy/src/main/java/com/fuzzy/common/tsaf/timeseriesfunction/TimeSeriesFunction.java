package com.fuzzy.common.tsaf.timeseriesfunction;

import lombok.Data;

import java.util.List;

@Data
public class TimeSeriesFunction {
    private String functionType;
    private List<String> args;

    public TimeSeriesFunction(String functionType, List<String> args) {
        this.functionType = functionType;
        this.args = args;
    }

    public String combinedArgs(String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append(this.functionType).append("(").append(columnName);
        for (int i = 1; i < this.args.size(); i++) {
            sb.append(",").append(this.args.get(i));
        }
        sb.append(")");
        return sb.toString();
    }
}
