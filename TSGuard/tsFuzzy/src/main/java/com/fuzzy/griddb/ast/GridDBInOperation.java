package com.fuzzy.griddb.ast;


import com.fuzzy.common.gen.ReGenerateExpressionException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class GridDBInOperation implements GridDBExpression {

    private final GridDBExpression expr;
    private final List<GridDBExpression> listElements;
    private final boolean isTrue;

    public GridDBInOperation(GridDBExpression expr, List<GridDBExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public GridDBExpression getExpr() {
        return expr;
    }

    public List<GridDBExpression> getListElements() {
        return listElements;
    }

    @Override
    public GridDBConstant getExpectedValue() {
        GridDBConstant leftVal = expr.getExpectedValue();
        if (leftVal.isNull()) return GridDBConstant.createNullConstant();

        for (GridDBExpression rightExpr : listElements) {
            GridDBConstant rightVal = rightExpr.getExpectedValue();
            GridDBConstant isEquals = leftVal.isEquals(rightVal);
            if (isEquals.isNull()) return GridDBConstant.createNullConstant();
            else if (isEquals.asBooleanNotNull()) return GridDBConstant.createBoolean(isTrue);
        }
        return GridDBConstant.createBoolean(!isTrue);
    }

    public boolean isTrue() {
        return isTrue;
    }

    @Override
    public void checkSyntax() {
        for (GridDBExpression rightExpr : listElements) {
            if (!GridDBConstant.dataTypeIsEqual(expr.getExpectedValue(), rightExpr.getExpectedValue())) {
                log.warn("dataType is not equal");
                throw new ReGenerateExpressionException("IN");
            }
        }
    }

    @Override
    public boolean hasColumn() {
        for (int i = 0; i < listElements.size(); i++) {
            if (listElements.get(i).hasColumn()) return true;
        }

        return expr.hasColumn();
    }
}
