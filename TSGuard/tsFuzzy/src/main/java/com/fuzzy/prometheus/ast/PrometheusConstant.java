package com.fuzzy.prometheus.ast;

import com.fuzzy.common.util.BigDecimalUtil;
import com.fuzzy.prometheus.PrometheusSchema.CommonDataType;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.math.BigInteger;

@Slf4j
public abstract class PrometheusConstant implements PrometheusExpression {

    public boolean isInt() {
        return false;
    }

    public boolean isNull() {
        return false;
    }

    public boolean isBoolean() {
        return false;
    }

    public boolean isDouble() {
        return false;
    }

    public boolean isBigDecimal() {
        return false;
    }

    public boolean isNumber() {
        return false;
    }

    public abstract static class PrometheusNoPQSConstant extends PrometheusConstant {

        @Override
        public boolean asBooleanNotNull() {
            throw throwException();
        }

        private RuntimeException throwException() {
            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
        }

        @Override
        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
            return null;
        }

        @Override
        public String castAsString() {
            throw throwException();

        }

        @Override
        public CommonDataType getType() {
            throw throwException();
        }

        @Override
        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
            throw throwException();
        }

    }

    public static class PrometheusDoubleConstant extends PrometheusConstant {
        public final static int scale = 7;
        private final double value;
        private final String stringRepresentation;

        public PrometheusDoubleConstant(double value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
        }

        public PrometheusDoubleConstant(double value) {
            this.value = value;
            this.stringRepresentation = Double.toString(value);
        }

        @Override
        public boolean isDouble() {
            return true;
        }

        @Override
        public boolean isNumber() {
            return true;
        }

        @Override
        public double getDouble() {
            return value;
        }

        @Override
        public long getInt() {
            return (long) Math.floor(value);
        }

        @Override
        public boolean asBooleanNotNull() {
            return false;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public String castAsString() {
            return String.valueOf(value);
        }

        @Override
        public CommonDataType getType() {
            return CommonDataType.DOUBLE;
        }

        @Override
        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
            if (rightVal.isNull()) {
                return PrometheusConstant.createNullConstant();
            } /*else if (rightVal.isDouble() || rightVal.isBoolean() || rightVal.isInt() || rightVal.isBigDecimal() ||
                    rightVal.isString()) {
                return castAs(PrometheusCastOperation.CastType.BIGDECIMAL).isEquals(rightVal);
            } */ else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
            if (rightVal.isNull()) {
                return PrometheusConstant.createNullConstant();
            } /*else if (rightVal.isDouble() || rightVal.isBoolean() || rightVal.isInt() || rightVal.isBigDecimal() ||
                    rightVal.isString()) {
                return castAs(PrometheusCastOperation.CastType.BIGDECIMAL).isLessThan(rightVal);
            } */ else {
                throw new AssertionError(rightVal);
            }
        }
    }

//    public static class PrometheusTextConstant extends PrometheusConstant {
//
//        private final String value;
//        private final boolean singleQuotes;
//
//        public PrometheusTextConstant(String value) {
//            this.value = value;
//            singleQuotes = false;
//        }
//
//        public PrometheusTextConstant(String value, boolean singleQuotes) {
//            this.value = value;
//            this.singleQuotes = singleQuotes;
//        }
//
//        private void checkIfSmallFloatingPointText() {
////            boolean isSmallFloatingPointText = isString() && asBooleanNotNull()
////                    && castAs(CastType.SIGNED).getInt() == 0;
////            if (isSmallFloatingPointText) {
////                throw new IgnoreMeException();
////            }
//        }
//
//        @Override
//        public boolean asBooleanNotNull() {
//            return false;
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            StringBuilder sb = new StringBuilder();
//            String quotes = singleQuotes ? "'" : "\"";
//            sb.append(quotes);
//            String text = value.replace(quotes, quotes + quotes).replace("\\", "\\\\");
//            sb.append(text);
//            sb.append(quotes);
//            return sb.toString();
//        }
//
//        @Override
//        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
//            if (rightVal.isNull()) {
//                return PrometheusConstant.createNullConstant();
//            } else if (rightVal.isString()) {
//                return PrometheusConstant.createBoolean(StringUtils.equals(value, rightVal.getString()));
//            } else if (rightVal.isInt()) {
//                PrometheusConstant castIntVal = castAs(CastType.CommonDataTypeToCastType(rightVal.getType()));
//                return PrometheusConstant.createBoolean(new BigInteger(((PrometheusIntConstant) castIntVal).getStringRepr())
//                        .compareTo(new BigInteger(((PrometheusIntConstant) rightVal).getStringRepr())) == 0);
//            } else if (rightVal.isBoolean()) {
//                PrometheusConstant castIntVal = castAs(CastType.CommonDataTypeToCastType(rightVal.getType()));
//                return PrometheusConstant.createBoolean(castIntVal.asBooleanNotNull() == rightVal.asBooleanNotNull());
//            } else {
//                // TODO float、double
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public String getString() {
//            return value;
//        }
//
//        @Override
//        public boolean isString() {
//            return true;
//        }
//
//        @Override
//        public PrometheusConstant castAs(CastType type) {
//            try {
//                switch (type) {
//                    case BOOLEAN:
//                        if (PrometheusValueStateConstant.TRUE.getValue().equalsIgnoreCase(value))
//                            return new PrometheusIntConstant(true);
//                        else if (PrometheusValueStateConstant.FALSE.getValue().equalsIgnoreCase(value))
//                            return new PrometheusIntConstant(false);
//                        else {
//                            log.info("不支持将该String转为Boolean值, str:{}", value);
//                            throw new IgnoreMeException();
//                        }
//                    case INT32:
//                        return new PrometheusIntConstant(Integer.parseInt(value));
//                    case INT64:
//                        return new PrometheusIntConstant(Long.parseLong(value), CommonDataType.INT64);
//                    case TEXT:
//                        return this;
//                    case FLOAT:
//                    case DOUBLE:
//                    default:
//                        throw new AssertionError();
//                }
//            } catch (NumberFormatException e) {
//                // parse text error
//                throw new IgnoreMeException();
//            }
//        }
//
//        @Override
//        public String castAsString() {
//            return value;
//        }
//
//        @Override
//        public CommonDataType getType() {
//            return CommonDataType.TEXT;
//        }
//
//        @Override
//        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
//            try {
//                if (CommonDataType.INT32.equals(rightVal.getType())) {
//                    return new PrometheusIntConstant(Integer.parseInt(value) < rightVal.getInt());
//                } else if (CommonDataType.INT64.equals(rightVal.getType())) {
//                    return new PrometheusIntConstant(Long.parseLong(value) < rightVal.getInt());
//                } else if (rightVal.isString()) {
//                    return new PrometheusIntConstant(StringUtils.compare(value, rightVal.getString()) < 0);
//                } else {
//                    // TODO float、double
//                    throw new AssertionError(rightVal);
//                }
//            } catch (NumberFormatException e) {
//                // parse text error
//                throw new IgnoreMeException();
//            }
//        }
//
//    }

    public static class PrometheusIntConstant extends PrometheusConstant {

        private final long value;
        private final String stringRepresentation;
        private final CommonDataType dataType;

        public PrometheusIntConstant(long value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
            dataType = CommonDataType.INT;
        }

        public PrometheusIntConstant(long value) {
            this.value = value;
            this.stringRepresentation = String.valueOf(value);
            dataType = CommonDataType.INT;
        }

        public PrometheusIntConstant(boolean booleanValue) {
            this.value = booleanValue ? 1 : 0;
            this.stringRepresentation = booleanValue ? "TRUE" : "FALSE";
            dataType = CommonDataType.BOOLEAN;
        }

        @Override
        public boolean isInt() {
            return CommonDataType.INT.equals(dataType);
        }

        @Override
        public boolean isNumber() {
            return true;
        }

        @Override
        public long getInt() {
            switch (dataType) {
                case INT:
                    return (int) value;
                default:
                    throw new UnsupportedOperationException(String.format("PrometheusIntConstant不支持该数据类型:%s!", dataType));
            }
        }

        @Override
        public boolean asBooleanNotNull() {
            return isBoolean() && this.value != 0;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
            if (rightVal.isInt()) {
                return PrometheusConstant.createBoolean(new BigInteger(getStringRepr())
                        .compareTo(new BigInteger(((PrometheusIntConstant) rightVal).getStringRepr())) == 0);
            } else if (rightVal.isNull()) {
                return PrometheusConstant.createNullConstant();
            } else {
                throw new AssertionError(rightVal);
            }
        }

//        @Override
//        public PrometheusConstant castAs(CastType type) {
//            switch (type) {
//                case BOOLEAN:
//                    return new PrometheusIntConstant(value != 0);
//                case INT32:
//                    if (CommonDataType.INT64.equals(this.dataType))
//                        throw new UnsupportedOperationException("INT64不支持转换为INT32");
//                    return new PrometheusIntConstant(value);
//                case INT64:
//                    return new PrometheusIntConstant(value, CommonDataType.INT64);
//                case TEXT:
//                    return new PrometheusTextConstant(String.valueOf(value));
//                case FLOAT:
//                case DOUBLE:
//                default:
//                    throw new AssertionError();
//            }
//        }

        @Override
        public String castAsString() {
            return String.valueOf(value);
        }

        @Override
        public CommonDataType getType() {
            return this.dataType;
        }

        private String getStringRepr() {
            return String.valueOf(value);
        }

        @Override
        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
            if (rightVal.isInt()) {
                return new PrometheusIntConstant(value < rightVal.getInt());
            } /*else if (rightVal.isString()) {
                return new PrometheusIntConstant(value < rightVal.castAs(CastType.INT64).getInt());
            }*/ else {
                // TODO float、double
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class PrometheusNullConstant extends PrometheusConstant {

        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public boolean asBooleanNotNull() {
            throw new UnsupportedOperationException(this.toString());
        }

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

        @Override
        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
            return PrometheusConstant.createNullConstant();
        }

        @Override
        public String castAsString() {
            return "NULL";
        }

        @Override
        public CommonDataType getType() {
            return CommonDataType.NULL;
        }

        @Override
        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
            return this;
        }
    }

    public static class PrometheusBigDecimalConstant extends PrometheusConstant {
        private final BigDecimal value;
        private final String stringRepresentation;

        public PrometheusBigDecimalConstant(BigDecimal value) {
            this.value = new BigDecimal(value.toPlainString());
            if (isInt()) this.stringRepresentation = value.stripTrailingZeros().toPlainString();
            else this.stringRepresentation = value.toPlainString();
        }

        @Override
        public boolean isBigDecimal() {
            return true;
        }

        @Override
        public boolean isDouble() {
            return BigDecimalUtil.isDouble(value);
        }

        @Override
        public boolean isInt() {
            return !BigDecimalUtil.isDouble(value);
        }

        @Override
        public boolean isNumber() {
            return true;
        }

        @Override
        public BigDecimal getBigDecimalValue() {
            return value;
        }

        @Override
        public double getDouble() {
            return value.doubleValue();
        }

        @Override
        public long getInt() {
            return value.longValue();
        }

        @Override
        public boolean asBooleanNotNull() {
            return false;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public String castAsString() {
            return stringRepresentation;
        }

        @Override
        public CommonDataType getType() {
            return CommonDataType.BIGDECIMAL;
        }

        @Override
        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
            if (rightVal.isNull()) {
                return PrometheusConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return PrometheusConstant.createFalse();
            } /*else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isString() || rightVal.isBigDecimal()) {
                return PrometheusConstant.createBoolean(value.subtract(
                                rightVal.castAs(PrometheusCastOperation.CastType.BIGDECIMAL).getBigDecimalValue())
                        .abs().compareTo(BigDecimal.valueOf(Math.pow(10, -PrometheusDoubleConstant.scale))) <= 0);
            }*/ else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
            if (rightVal.isNull()) {
                return PrometheusConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return PrometheusConstant.createFalse();
            } /*else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isString() || rightVal.isBigDecimal()) {
                return PrometheusConstant.createBoolean(value.compareTo(
                        rightVal.castAs(PrometheusCastOperation.CastType.BIGDECIMAL).getBigDecimalValue()) < 0);
            }*/ else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public double getDouble() {
        throw new UnsupportedOperationException();
    }

    public long getInt() {
        throw new UnsupportedOperationException();
    }

    public boolean isSigned() {
        return false;
    }

    public boolean hasSuffix() {
        return false;
    }

    public String getString() {
        throw new UnsupportedOperationException();
    }

    public boolean isString() {
        return false;
    }

    public static PrometheusConstant createNullConstant() {
        // TODO 空值问题
        return new PrometheusNullConstant();
    }

    /*public static PrometheusConstant createBooleanConstant(boolean value) {
        return new PrometheusBooleanConstant(value);
    }*/

    public static PrometheusConstant createIntConstant(long value) {
        return new PrometheusIntConstant(value);
    }

    public static PrometheusConstant createBooleanIntConstant(boolean value) {
        return new PrometheusIntConstant(value);
    }

    public static PrometheusConstant createIntConstantNotAsBoolean(long value) {
        return new PrometheusIntConstant(value, String.valueOf(value));
    }

    public BigDecimal getBigDecimalValue() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PrometheusConstant getExpectedValue() {
        return this;
    }

    public abstract boolean asBooleanNotNull();

    public abstract String getTextRepresentation();

    public static PrometheusConstant createFalse() {
        return PrometheusConstant.createBooleanIntConstant(false);
    }

    public static PrometheusConstant createBoolean(boolean isTrue) {
        return PrometheusConstant.createBooleanIntConstant(isTrue);
    }

    public static PrometheusConstant createTrue() {
        return PrometheusConstant.createBooleanIntConstant(true);
    }

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract PrometheusConstant isEquals(PrometheusConstant rightVal);

//    public abstract PrometheusConstant castAs(CastType type);

    public abstract String castAsString();

    public abstract CommonDataType getType();

    protected abstract PrometheusConstant isLessThan(PrometheusConstant rightVal);

    public static PrometheusConstant createBigDecimalConstant(BigDecimal value) {
        return new PrometheusConstant.PrometheusBigDecimalConstant(value);
    }

    public static PrometheusConstant createDoubleConstant(double value) {
        return new PrometheusConstant.PrometheusDoubleConstant(value);
    }

    public static BigDecimal createFloatArithmeticTolerance() {
        return BigDecimal.valueOf(Math.pow(10, -PrometheusConstant.PrometheusDoubleConstant.scale));
    }
}
