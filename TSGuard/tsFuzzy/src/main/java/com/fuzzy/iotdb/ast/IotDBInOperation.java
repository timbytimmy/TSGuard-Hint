package com.fuzzy.iotdb.ast;


import com.fuzzy.common.gen.ReGenerateExpressionException;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class IotDBInOperation implements IotDBExpression {

    private final IotDBExpression expr;
    private final List<IotDBExpression> listElements;
    private final boolean isTrue;

    public IotDBInOperation(IotDBExpression expr, List<IotDBExpression> listElements, boolean isTrue) {
        this.expr = expr;
        this.listElements = listElements;
        this.isTrue = isTrue;
    }

    public IotDBExpression getExpr() {
        return expr;
    }

    public List<IotDBExpression> getListElements() {
        return listElements;
    }

    @Override
    public IotDBConstant getExpectedValue() {
        IotDBConstant leftVal = expr.getExpectedValue();
        if (leftVal.isNull()) return IotDBConstant.createNullConstant();

        boolean isNull = false;
        for (IotDBExpression rightExpr : listElements) {
            IotDBConstant rightVal = rightExpr.getExpectedValue();
            IotDBConstant isEquals = leftVal.isEquals(rightVal);
            if (isEquals.isNull()) isNull = true;
            else {
                if (isEquals.asBooleanNotNull()) return IotDBConstant.createBoolean(isTrue);
            }
        }
        if (isNull) return IotDBConstant.createNullConstant();
        else {
            return IotDBConstant.createBoolean(!isTrue);
        }
    }

    public boolean isTrue() {
        return isTrue;
    }

    @Override
    public void checkSyntax() {
        // 301. cannot be cast to
        for (IotDBExpression rightExpr : listElements) {
            if (!IotDBConstant.dataTypeIsEqual(expr.getExpectedValue(), rightExpr.getExpectedValue())) {
                log.warn("dataType is not equal");
                throw new ReGenerateExpressionException("IN");
            }
        }
    }
}
