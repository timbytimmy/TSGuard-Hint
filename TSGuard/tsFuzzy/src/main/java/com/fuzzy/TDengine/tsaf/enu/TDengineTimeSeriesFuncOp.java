package com.fuzzy.TDengine.tsaf.enu;

import java.math.BigDecimal;

public enum TDengineTimeSeriesFuncOp {
    LT, GT, LE, GE, NE, EQ;

    public boolean compare(BigDecimal a, BigDecimal b) {
        int result = a.compareTo(b);
        switch (this) {
            case LT:
                return result < 0;
            case LE:
                return result <= 0;
            case GT:
                return result > 0;
            case GE:
                return result >= 0;
            case EQ:
                return result == 0;
            case NE:
                return result != 0;
            default:
                throw new IllegalArgumentException("Unsupported operator: " + this);
        }
    }
}
