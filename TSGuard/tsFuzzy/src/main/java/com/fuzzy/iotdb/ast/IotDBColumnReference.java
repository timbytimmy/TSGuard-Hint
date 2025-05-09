package com.fuzzy.iotdb.ast;


import com.fuzzy.iotdb.IotDBSchema.IotDBColumn;

public class IotDBColumnReference implements IotDBExpression {

    private final IotDBColumn column;
    private final IotDBConstant value;

    public IotDBColumnReference(IotDBColumn column, IotDBConstant value) {
        this.column = column;
        this.value = value;
    }

    public static IotDBColumnReference create(IotDBColumn column, IotDBConstant value) {
        return new IotDBColumnReference(column, value);
    }

    public IotDBColumn getColumn() {
        return column;
    }

    public IotDBConstant getValue() {
        return value;
    }

    @Override
    public IotDBConstant getExpectedValue() {
        return value;
    }

}
