package com.fuzzy.iotdb.ast;

import com.fuzzy.common.ast.SelectBase;

import java.util.List;

public class IotDBSelect extends SelectBase<IotDBExpression> implements IotDBExpression {

    private SelectType fromOptions = SelectType.ALL;
    private List<IotDBCastOperation> castColumns;

    public enum SelectType {
        DISTINCT, ALL, DISTINCTROW;
    }

    public void setSelectType(SelectType fromOptions) {
        this.setFromOptions(fromOptions);
    }

    public SelectType getFromOptions() {
        return fromOptions;
    }

    public void setFromOptions(SelectType fromOptions) {
        this.fromOptions = fromOptions;
    }

    public List<IotDBCastOperation> getCastColumns() {
        return castColumns;
    }

    public void setCastColumns(List<IotDBCastOperation> castColumns) {
        this.castColumns = castColumns;
    }

    @Override
    public IotDBConstant getExpectedValue() {
        return null;
    }

}
