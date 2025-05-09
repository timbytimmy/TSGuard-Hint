package com.fuzzy.influxdb.ast;


import com.fuzzy.common.ast.SelectBase;

import java.util.Collections;
import java.util.List;

public class InfluxDBSelect extends SelectBase<InfluxDBExpression> implements InfluxDBExpression {

    private SelectType fromOptions = SelectType.ALL;
    private List<String> modifiers = Collections.emptyList();

    public enum SelectType {
        DISTINCT, ALL, DISTINCTROW;
    }

    public void setSelectType(SelectType fromOptions) {
        this.setFromOptions(fromOptions);
    }

    public SelectType getFromOptions() {
        return fromOptions;
    }

    public void setFromOptions(SelectType fromOptions) {
        this.fromOptions = fromOptions;
    }

    public void setModifiers(List<String> modifiers) {
        this.modifiers = modifiers;
    }

    public List<String> getModifiers() {
        return modifiers;
    }

    @Override
    public InfluxDBConstant getExpectedValue() {
        return null;
    }

}
