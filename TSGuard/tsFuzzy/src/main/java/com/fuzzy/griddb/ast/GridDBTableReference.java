package com.fuzzy.griddb.ast;


import com.fuzzy.griddb.GridDBSchema.GridDBTable;

public class GridDBTableReference implements GridDBExpression {

    private final GridDBTable table;

    public GridDBTableReference(GridDBTable table) {
        this.table = table;
    }

    public GridDBTable getTable() {
        return table;
    }

}
