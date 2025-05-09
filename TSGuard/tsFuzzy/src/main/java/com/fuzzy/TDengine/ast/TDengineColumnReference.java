package com.fuzzy.TDengine.ast;


import com.fuzzy.TDengine.TDengineSchema.TDengineColumn;

public class TDengineColumnReference implements TDengineExpression {

    private final TDengineColumn column;
    private final TDengineConstant value;

    public TDengineColumnReference(TDengineColumn column, TDengineConstant value) {
        this.column = column;
        this.value = value;
    }

    public static TDengineColumnReference create(TDengineColumn column, TDengineConstant value) {
        return new TDengineColumnReference(column, value);
    }

    public TDengineColumn getColumn() {
        return column;
    }

    public TDengineConstant getValue() {
        return value;
    }

    @Override
    public TDengineConstant getExpectedValue() {
        return value;
    }

}
