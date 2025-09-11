package com.fuzzy.common.schema;

import java.util.List;

public class TimeSeriesTemplate<U> {

    private final String templateString;
    private final List<U> parameterTypes;

    public TimeSeriesTemplate(String templateString, List<U> parameterTypes) {
        this.templateString = templateString;
        this.parameterTypes = parameterTypes;
    }

    public String getTemplateString() {
        return templateString;
    }

    public List<U> getParameterTypes() {
        return parameterTypes;
    }
}
