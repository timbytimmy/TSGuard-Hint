package com.fuzzy.TDengine.ast;

import com.fuzzy.IgnoreMeException;
import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.function.BinaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

@Slf4j
public class TDengineComputableFunction implements TDengineExpression {

    private final TDengineFunction func;
    private final TDengineExpression[] args;

    public TDengineComputableFunction(TDengineFunction func, TDengineExpression... args) {
        this.func = func;
        this.args = args.clone();
    }

    public TDengineFunction getFunction() {
        return func;
    }

    public TDengineExpression[] getArguments() {
        return args.clone();
    }

    public enum TDengineFunction {

        ABS(1, "ABS") {
            @Override
            public TDengineConstant apply(TDengineConstant[] args, TDengineExpression... origArgs) {
                if (args[0].isNull()) return TDengineConstant.createNullConstant();
                BigDecimal val = args[0].castAs(TDengineCastOperation.CastType.BIGDECIMAL).getBigDecimalValue();
                return mathApply(val, Math::abs);
            }

            @Override
            public TDengineConstant limitValueRange(TDengineConstant value) {
                return value;
            }
        },
        ACOS(1, "ACOS") {
            @Override
            public TDengineConstant apply(TDengineConstant[] args, TDengineExpression... origArgs) {
                if (args[0].isNull()) return TDengineConstant.createNullConstant();
                BigDecimal val = args[0].castAs(TDengineCastOperation.CastType.BIGDECIMAL).getBigDecimalValue();
                return mathApply(val, Math::acos);
            }

            @Override
            public TDengineConstant limitValueRange(TDengineConstant value) {
                if (value.isNull()) return value;
                BigDecimal bigDecimalValue = value.castAs(TDengineCastOperation.CastType.BIGDECIMAL).getBigDecimalValue();
                return TDengineConstant.createBigDecimalConstant(bigDecimalValue.subtract(
                        new BigDecimal(bigDecimalValue.toBigInteger())));
            }
        },
        ASIN(1, "ASIN") {
            @Override
            public TDengineConstant apply(TDengineConstant[] args, TDengineExpression... origArgs) {
                if (args[0].isNull()) return TDengineConstant.createNullConstant();
                BigDecimal val = args[0].castAs(TDengineCastOperation.CastType.BIGDECIMAL).getBigDecimalValue();
                return mathApply(val, Math::asin);
            }

            @Override
            public TDengineConstant limitValueRange(TDengineConstant value) {
                if (value.isNull()) return value;
                BigDecimal bigDecimalValue = value.castAs(TDengineCastOperation.CastType.BIGDECIMAL).getBigDecimalValue();
                return TDengineConstant.createBigDecimalConstant(bigDecimalValue.subtract(
                        new BigDecimal(bigDecimalValue.toBigInteger())));
            }
        },
        ATAN(1, "ATAN") {
            @Override
            public TDengineConstant apply(TDengineConstant[] args, TDengineExpression... origArgs) {
                if (args[0].isNull()) return TDengineConstant.createNullConstant();
                BigDecimal val = args[0].castAs(TDengineCastOperation.CastType.BIGDECIMAL).getBigDecimalValue();
                return mathApply(val, Math::atan);
            }

            @Override
            public TDengineConstant limitValueRange(TDengineConstant value) {
                if (value.isNull()) return value;
                BigDecimal bigDecimalValue = value.castAs(TDengineCastOperation.CastType.BIGDECIMAL).getBigDecimalValue();
                return TDengineConstant.createBigDecimalConstant(bigDecimalValue.remainder(new BigDecimal(1000)));
            }
        },
        SQRT(1, "SQRT") {
            @Override
            public TDengineConstant apply(TDengineConstant[] args, TDengineExpression... origArgs) {
                if (args[0].isNull()) return TDengineConstant.createNullConstant();
                BigDecimal val = args[0].castAs(TDengineCastOperation.CastType.BIGDECIMAL).getBigDecimalValue();
                return mathApply(val, Math::sqrt);
            }

            @Override
            public TDengineConstant limitValueRange(TDengineConstant value) {
                if (value.isNull()) return value;
                BigDecimal bigDecimalValue = value.castAs(TDengineCastOperation.CastType.BIGDECIMAL).getBigDecimalValue();
                return TDengineConstant.createBigDecimalConstant(bigDecimalValue.abs());
            }
        },
        ;

        private String functionName;
        final int nrArgs;
        private final boolean variadic;

        private static TDengineConstant aggregate(TDengineConstant[] evaluatedArgs, BinaryOperator<TDengineConstant> op) {
            boolean containsNull = Stream.of(evaluatedArgs).anyMatch(TDengineConstant::isNull);
            if (containsNull) return TDengineConstant.createNullConstant();
            TDengineConstant least = Randomly.fromOptions(evaluatedArgs);
            for (TDengineConstant arg : evaluatedArgs) {
                TDengineConstant val = arg.castAs(TDengineCastOperation.CastType.BIGDECIMAL);
                least = op.apply(least, val);
            }
            return least;
        }

        private static TDengineConstant mathApply(BigDecimal val, UnaryOperator<Double> op) {
            try {
                BigDecimal value = BigDecimal.valueOf(op.apply(val.doubleValue()));
                return TDengineConstant.createBigDecimalConstant(value);
            } catch (ArithmeticException e) {
                log.warn("mathApply e:", e);
                throw new IgnoreMeException();
            }
        }

        TDengineFunction(int nrArgs, String functionName) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = false;
        }

        TDengineFunction(int nrArgs, String functionName, boolean variadic) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = variadic;
        }

        /**
         * Gets the number of arguments if the function is non-variadic. If the function is variadic, the minimum number
         * of arguments is returned.
         *
         * @return the number of arguments
         */
        public int getNrArgs() {
            return nrArgs;
        }

        public abstract TDengineConstant apply(TDengineConstant[] evaluatedArgs, TDengineExpression... args);

        public abstract TDengineConstant limitValueRange(TDengineConstant value);

        public static TDengineFunction getRandomFunction() {
            return Randomly.fromOptions(values());
        }

        @Override
        public String toString() {
            return functionName;
        }

        public boolean isVariadic() {
            return variadic;
        }

        public String getName() {
            return functionName;
        }
    }

    @Override
    public TDengineConstant getExpectedValue() {
        TDengineConstant[] constants = new TDengineConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i].getExpectedValue() == null) return null;
        }
        return func.apply(constants, args);
    }

    @Override
    public void checkSyntax() {
        if ((func == TDengineFunction.SQRT &&
                ((TDengineConstant) args[0]).castAs(TDengineCastOperation.CastType.BIGDECIMAL).getBigDecimalValue()
                        .compareTo(BigDecimal.ZERO) < 0)) {
            throw new ReGenerateExpressionException("TDengineComputableFunction");
        }
    }

}
