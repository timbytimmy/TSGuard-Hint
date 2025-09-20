package com.fuzzy.common.tsaf;

import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Data
@Builder
public class RangeConstraint {
    private BigDecimal greaterEqualValue = new BigDecimal(Long.MIN_VALUE);
    private BigDecimal lessEqualValue = new BigDecimal(Long.MAX_VALUE);

    public RangeConstraint() {
    }

    public RangeConstraint(BigDecimal greaterEqualValue, BigDecimal lessEqualValue) {
        this.greaterEqualValue = greaterEqualValue;
        this.lessEqualValue = lessEqualValue;
    }

    public RangeConstraint intersects(RangeConstraint constraint) {
        BigDecimal greaterEqualValue = this.greaterEqualValue.max(constraint.getGreaterEqualValue());
        BigDecimal lessEqualValue = this.lessEqualValue.min(constraint.getLessEqualValue());
        if (lessEqualValue.compareTo(greaterEqualValue) < 0) return null;
        else return new RangeConstraint(greaterEqualValue, lessEqualValue);
    }
}
