package com.fuzzy.griddb.ast;


import com.fuzzy.griddb.GridDBSchema;

public class GridDBSchemaReference implements GridDBExpression {

    private final GridDBSchema schema;

    public GridDBSchemaReference(GridDBSchema schema) {
        this.schema = schema;
    }

    public GridDBSchema getSchema() {
        return schema;
    }

}
