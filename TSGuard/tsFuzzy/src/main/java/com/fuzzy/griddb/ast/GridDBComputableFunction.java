package com.fuzzy.griddb.ast;

import com.fuzzy.Randomly;
import com.fuzzy.common.gen.ReGenerateExpressionException;
import com.fuzzy.common.tsaf.aggregation.DoubleArithmeticPrecisionConstant;
import com.fuzzy.griddb.GridDBSchema.GridDBDataType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

public class GridDBComputableFunction implements GridDBExpression {

    private final GridDBFunction func;
    private final GridDBExpression[] args;

    public GridDBComputableFunction(GridDBFunction func, GridDBExpression... args) {
        this.func = func;
        this.args = args.clone();
    }

    public GridDBFunction getFunction() {
        return func;
    }

    public GridDBExpression[] getArguments() {
        return args.clone();
    }

    public enum GridDBFunction {

        ABS(1, "ABS") {
            @Override
            public GridDBConstant apply(GridDBConstant[] args, GridDBExpression... origArgs) {
                if (args[0].isNull()) return GridDBConstant.createNullConstant();
                GridDBConstant val = args[0].castAs(GridDBDataType.BIGDECIMAL);
                return GridDBConstant.createBigDecimalConstant(val.getBigDecimalValue().abs());
            }
        },
        //        ROUND(2, "ROUND") {
//            @Override
//            public GridDBConstant apply(GridDBConstant[] args, GridDBExpression... origArgs) {
//                if (args[0].isNull()) return GridDBConstant.createNullConstant();
//                GridDBConstant val = args[0].castAs(GridDBDataType.BIGDECIMAL);
//                int roundPrecision = (int) args[1].castAs(GridDBDataType.BIGDECIMAL).getInt() % 6;
//                return GridDBConstant.createBigDecimalConstant(
//                        val.getBigDecimalValue().setScale(roundPrecision, RoundingMode.HALF_UP));
//            }
//        },
        MAX(2, "MAX") {
            @Override
            public GridDBConstant apply(GridDBConstant[] evaluatedArgs, GridDBExpression... args) {
                return aggregate(evaluatedArgs, (max, cur) -> cur.isLessThan(max).asBooleanNotNull() ? max : cur);
            }
        },
        MIN(2, "MIN", true) {
            @Override
            public GridDBConstant apply(GridDBConstant[] evaluatedArgs, GridDBExpression... args) {
                return aggregate(evaluatedArgs, (min, cur) -> cur.isLessThan(min).asBooleanNotNull() ? cur : min);
            }
        },
        SQRT(1, "SQRT") {
            @Override
            public GridDBConstant apply(GridDBConstant[] evaluatedArgs, GridDBExpression... args) {
                if (evaluatedArgs[0].isNull()) return GridDBConstant.createNullConstant();
                GridDBConstant val = evaluatedArgs[0].castAs(GridDBDataType.BIGDECIMAL);
                return GridDBConstant.createBigDecimalConstant(BigDecimal.valueOf(Math.sqrt(val.getDouble()))
                        .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
            }
        },
//        LOG(2, "LOG") {
//            @Override
//            public GridDBConstant apply(GridDBConstant[] evaluatedArgs, GridDBExpression... args) {
//                if (evaluatedArgs[0].isNull()) return GridDBConstant.createNullConstant();
//                GridDBConstant valLeft = evaluatedArgs[0].castAs(GridDBDataType.BIGDECIMAL);
//                GridDBConstant valRight = evaluatedArgs[1].castAs(GridDBDataType.BIGDECIMAL);
//                return GridDBConstant.createBigDecimalConstant(
//                        BigDecimal.valueOf(Math.log(valRight.getDouble()) / Math.log(valLeft.getDouble()))
//                                .setScale(DoubleArithmeticPrecisionConstant.scale, RoundingMode.HALF_UP));
//            }
//        },
        // BENCHMARK(2, "BENCHMARK") {
        //
        // @Override
        // public GridDBConstant apply(GridDBConstant[] evaluatedArgs, GridDBExpression[] args) {
        // if (evaluatedArgs[0].isNull()) {
        // return GridDBConstant.createNullConstant();
        // }
        // if (evaluatedArgs[0].castAs(CastType.SIGNED).getInt() < 0) {
        // return GridDBConstant.createNullConstant();
        // }
        // if (Math.abs(evaluatedArgs[0].castAs(CastType.SIGNED).getInt()) > 10) {
        // throw new IgnoreMeException();
        // }
        // return GridDBConstant.createIntConstant(0);
        // }
        //
        // },
//        COALESCE(2, "COALESCE") {
//            @Override
//            public GridDBConstant apply(GridDBConstant[] args, GridDBExpression... origArgs) {
//                GridDBConstant result = GridDBConstant.createNullConstant();
//                for (GridDBConstant arg : args) {
//                    if (!arg.isNull()) {
//                        result = GridDBConstant.createStringConstant(arg.castAsString());
//                        break;
//                    }
//                }
//                return castToMostGeneralType(result, origArgs);
//            }
//
//            @Override
//            public boolean isVariadic() {
//                return true;
//            }
//
//        },
//        IF(3, "IF") {
//            @Override
//            public GridDBConstant apply(GridDBConstant[] args, GridDBExpression... origArgs) {
//                GridDBConstant cond = args[0];
//                GridDBConstant left = args[1];
//                GridDBConstant right = args[2];
//                GridDBConstant result;
//                if (cond.isNull() || !cond.asBooleanNotNull()) {
//                    result = right;
//                } else {
//                    result = left;
//                }
//                return castToMostGeneralType(result, new GridDBExpression[]{origArgs[1], origArgs[2]});
//
//            }
//
//        },
//        IFNULL(2, "IFNULL") {
//            @Override
//            public GridDBConstant apply(GridDBConstant[] args, GridDBExpression... origArgs) {
//                GridDBConstant result;
//                if (args[0].isNull()) {
//                    result = args[1];
//                } else {
//                    result = args[0];
//                }
//                return castToMostGeneralType(result, origArgs);
//            }
//
//        },
//        LEAST(2, "LEAST", true) {
//            @Override
//            public GridDBConstant apply(GridDBConstant[] evaluatedArgs, GridDBExpression... args) {
//                return aggregate(evaluatedArgs, (min, cur) -> cur.isLessThan(min).asBooleanNotNull() ? cur : min);
//            }
//
//        },
//        GREATEST(2, "GREATEST", true) {
//            @Override
//            public GridDBConstant apply(GridDBConstant[] evaluatedArgs, GridDBExpression... args) {
//                return aggregate(evaluatedArgs, (max, cur) -> cur.isLessThan(max).asBooleanNotNull() ? max : cur);
//            }
//        }
        ;

        private String functionName;
        final int nrArgs;
        private final boolean variadic;

        private static GridDBConstant aggregate(GridDBConstant[] evaluatedArgs, BinaryOperator<GridDBConstant> op) {
            boolean containsNull = Stream.of(evaluatedArgs).anyMatch(GridDBConstant::isNull);
            if (containsNull) return GridDBConstant.createNullConstant();
            GridDBConstant least = Randomly.fromOptions(evaluatedArgs);
            for (GridDBConstant arg : evaluatedArgs) {
                GridDBConstant val = arg.castAs(GridDBDataType.BIGDECIMAL);
                least = op.apply(least, val);
            }
            return least;
        }

        GridDBFunction(int nrArgs, String functionName) {
            this.nrArgs = nrArgs;
            this.functionName = functionName;
            this.variadic = false;
        }

        GridDBFunction(int nrArgs, String functionName, boolean variadic) {
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

        public abstract GridDBConstant apply(GridDBConstant[] evaluatedArgs, GridDBExpression... args);

        public static GridDBFunction getRandomFunction() {
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
    public GridDBConstant getExpectedValue() {
        GridDBConstant[] constants = new GridDBConstant[args.length];
        for (int i = 0; i < constants.length; i++) {
            constants[i] = args[i].getExpectedValue();
            if (constants[i].getExpectedValue() == null) return null;
        }
        return func.apply(constants, args);
    }

    @Override
    public void checkSyntax() {
        if ((func == GridDBFunction.SQRT &&
                ((GridDBConstant) args[0]).castAs(GridDBDataType.BIGDECIMAL).getBigDecimalValue()
                        .compareTo(BigDecimal.ZERO) < 0)) {
            throw new ReGenerateExpressionException("GridDBComputableFunction");
        }
    }

    @Override
    public boolean hasColumn() {
        for (int i = 0; i < args.length; i++)
            if (args[i].hasColumn()) return true;
        return false;
    }
}
