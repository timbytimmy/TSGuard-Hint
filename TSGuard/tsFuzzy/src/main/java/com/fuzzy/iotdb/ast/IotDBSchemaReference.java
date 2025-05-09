package com.fuzzy.iotdb.ast;


import com.fuzzy.iotdb.IotDBSchema;

public class IotDBSchemaReference implements IotDBExpression {

    private final IotDBSchema schema;

    public IotDBSchemaReference(IotDBSchema schema) {
        this.schema = schema;
    }

    public IotDBSchema getSchema() {
        return schema;
    }

}
