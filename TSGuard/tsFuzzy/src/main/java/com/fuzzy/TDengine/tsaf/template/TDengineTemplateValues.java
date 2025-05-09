package com.fuzzy.TDengine.tsaf.template;

import lombok.Data;

import java.util.List;

@Data
public class TDengineTemplateValues {
    private List<Object> values;

    public TDengineTemplateValues(List<Object> values) {
        this.values = values;
    }

    public int size() {
        return values.size();
    }

    public Object get(int index) {
        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException();
        }
        return values.get(index);
    }
}
