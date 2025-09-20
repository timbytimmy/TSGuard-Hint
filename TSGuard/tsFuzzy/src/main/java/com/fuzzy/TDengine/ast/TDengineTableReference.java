package com.fuzzy.TDengine.ast;


import com.fuzzy.TDengine.TDengineSchema.TDengineTable;

public class TDengineTableReference implements TDengineExpression {

    private final TDengineTable table;

    public TDengineTableReference(TDengineTable table) {
        this.table = table;
    }

    public TDengineTable getTable() {
        return table;
    }

}
