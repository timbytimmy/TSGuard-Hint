package com.fuzzy.iotdb.ast;


import com.fuzzy.iotdb.IotDBSchema.IotDBTable;

public class IotDBTableReference implements IotDBExpression {

    private final IotDBTable table;

    public IotDBTableReference(IotDBTable table) {
        this.table = table;
    }

    public IotDBTable getTable() {
        return table;
    }

}
