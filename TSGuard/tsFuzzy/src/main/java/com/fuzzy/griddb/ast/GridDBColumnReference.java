package com.fuzzy.griddb.ast;


import com.fuzzy.griddb.GridDBSchema.GridDBColumn;
import com.fuzzy.griddb.tsaf.enu.GridDBConstantString;

public class GridDBColumnReference implements GridDBExpression {

    private final GridDBColumn column;
    private final GridDBConstant value;

    public GridDBColumnReference(GridDBColumn column, GridDBConstant value) {
        this.column = column;
        this.value = value;
    }

    public static GridDBColumnReference create(GridDBColumn column, GridDBConstant value) {
        return new GridDBColumnReference(column, value);
    }

    public GridDBColumn getColumn() {
        return column;
    }

    public GridDBConstant getValue() {
        return value;
    }

    @Override
    public GridDBConstant getExpectedValue() {
        return value;
    }

    @Override
    public boolean hasColumn() {
        return !column.getName().equalsIgnoreCase(GridDBConstantString.TIME_FIELD_NAME.getName());
    }
}
