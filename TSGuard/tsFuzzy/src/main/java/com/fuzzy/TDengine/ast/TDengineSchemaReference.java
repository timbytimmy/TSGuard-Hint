package com.fuzzy.TDengine.ast;


import com.fuzzy.TDengine.TDengineSchema;

public class TDengineSchemaReference implements TDengineExpression {

    private final TDengineSchema schema;

    public TDengineSchemaReference(TDengineSchema schema) {
        this.schema = schema;
    }

    public TDengineSchema getSchema() {
        return schema;
    }

}
