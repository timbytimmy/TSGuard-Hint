package com.fuzzy.influxdb.ast;

import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.util.BigDecimalUtil;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBDataType;
import com.fuzzy.influxdb.ast.InfluxDBCastOperation.CastType;

import java.math.BigDecimal;

public abstract class InfluxDBConstant implements InfluxDBExpression {

    public boolean isInt() {
        return false;
    }

    public boolean isUInt() {
        return false;
    }

    public boolean isDouble() {
        return false;
    }

    public boolean isNull() {
        return false;
    }

    public boolean isBoolean() {
        return false;
    }

    public boolean isBigDecimal() {
        return false;
    }

    public boolean isNumber() {
        return false;
    }

    public boolean isTimestamp() {
        return false;
    }

    public abstract static class InfluxDBNoPQSConstant extends InfluxDBConstant {

        @Override
        public boolean asBooleanNotNull() {
            throw throwException();
        }

        private RuntimeException throwException() {
            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
        }

        @Override
        public InfluxDBConstant isEquals(InfluxDBConstant rightVal) {
            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
        }

        @Override
        public InfluxDBConstant castAs(CastType type) {
            throw throwException();
        }

        @Override
        public String castAsString() {
            throw throwException();

        }

        @Override
        public InfluxDBDataType getType() {
            throw throwException();
        }

        @Override
        protected InfluxDBConstant isLessThan(InfluxDBConstant rightVal) {
            throw throwException();
        }

    }

    public static class InfluxDBTextConstant extends InfluxDBConstant {

        private final String value;
        private final boolean singleQuotes;

        public InfluxDBTextConstant(String value) {
            this.value = value;
            singleQuotes = true;
        }

        public InfluxDBTextConstant(String value, boolean singleQuotes) {
            this.value = value;
            this.singleQuotes = singleQuotes;
        }

        private void checkIfSmallFloatingPointText() {
            boolean isSmallFloatingPointText = isString() && asBooleanNotNull()
                    && castAs(CastType.SIGNED).getInt() == 0;
            if (isSmallFloatingPointText) {
                throw new IgnoreMeException();
            }
        }

        @Override
        public boolean asBooleanNotNull() {
            return false;
        }

        @Override
        public String getTextRepresentation() {
            StringBuilder sb = new StringBuilder();
            String quotes = singleQuotes ? "'" : "\"";
            sb.append(quotes);
            String text = value.replace(quotes, quotes + quotes).replace("\\", "\\\\");
            sb.append(text);
            sb.append(quotes);
            return sb.toString();
        }

        @Override
        public InfluxDBConstant isEquals(InfluxDBConstant rightVal) {
            if (rightVal.isNull()) {
                return InfluxDBConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return InfluxDBConstant.createBoolean(value.equalsIgnoreCase(rightVal.getString()));
            } else if (rightVal.isInt() || rightVal.isDouble() || rightVal.isBigDecimal() || rightVal.isBoolean()) {
                return InfluxDBConstant.createBoolean(false);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public String getString() {
            return value;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public InfluxDBConstant castAs(CastType type) {
            try {
                switch (type) {
                    case STRING:
                    case FIELD:
                    case TAG:
                        return this;
                    case SIGNED:
                    case INTEGER:
                    case UNSIGNED:
                    case FLOAT:
                    case BOOLEAN:
                    case BIGDECIMAL:
                    default:
                        throw new IgnoreMeException();
                }
            } catch (NumberFormatException e) {
                throw new IgnoreMeException();
            }
        }

        @Override
        public String castAsString() {
            return value;
        }

        @Override
        public InfluxDBDataType getType() {
            return InfluxDBDataType.STRING;
        }

        @Override
        protected InfluxDBConstant isLessThan(InfluxDBConstant rightVal) {
            // 字符串比较 -> 全部FALSE
            return InfluxDBConstant.createBoolean(false);
        }
    }

    public static class InfluxDBIntConstant extends InfluxDBConstant {

        private final long value;
        private final String stringRepresentation;
        private final boolean isSigned;
        private final boolean isBoolean;
        private final boolean hasSuffix;
        private final InfluxDBDataType dataType;

        public InfluxDBIntConstant(long value, String stringRepresentation, boolean hasSuffix) {
            this.value = value;
            if (hasSuffix) {
                this.stringRepresentation = stringRepresentation + "i";
                this.hasSuffix = true;
            } else {
                this.stringRepresentation = stringRepresentation;
                this.hasSuffix = false;
            }
            this.dataType = InfluxDBDataType.INT;
            this.isSigned = true;
            this.isBoolean = false;
        }

        public InfluxDBIntConstant(long value, boolean isSigned, boolean hasSuffix) {
            this.value = value;
            this.isSigned = isSigned;
            if (isSigned && hasSuffix) {
                stringRepresentation = String.valueOf(value) + "i";
                this.hasSuffix = true;
            } else if (!isSigned && hasSuffix) {
                stringRepresentation = Long.toUnsignedString(value) + "u";
                this.hasSuffix = true;
            } else if (isSigned && !hasSuffix) {
                stringRepresentation = String.valueOf(value);
                this.hasSuffix = false;
            } else {
                stringRepresentation = Long.toUnsignedString(value);
                this.hasSuffix = false;
            }
            this.dataType = this.isSigned ? InfluxDBDataType.INT : InfluxDBDataType.UINT;
            this.isBoolean = false;
        }

        public InfluxDBIntConstant(boolean value) {
            this.value = value ? 1 : 0;
            this.isSigned = false;
            this.stringRepresentation = String.valueOf(value);
            this.isBoolean = true;
            this.hasSuffix = false;
            this.dataType = InfluxDBDataType.BOOLEAN;
        }

        @Override
        public boolean isInt() {
            return this.dataType == InfluxDBDataType.INT;
        }

        @Override
        public boolean isNumber() {
            return isInt() || isUInt() || isTimestamp();
        }

        @Override
        public boolean isUInt() {
            return this.dataType == InfluxDBDataType.UINT;
        }

        @Override
        public boolean isBoolean() {
            return this.dataType == InfluxDBDataType.BOOLEAN;
        }

        @Override
        public boolean isTimestamp() {
            return this.dataType == InfluxDBDataType.TIMESTAMP;
        }

        @Override
        public long getInt() {
            return value;
        }

        @Override
        public boolean asBooleanNotNull() {
            return isBoolean && this.value != 0;
//            return value != 0;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public InfluxDBConstant isEquals(InfluxDBConstant rightVal) {
            if (rightVal.isNull()) {
                return InfluxDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return InfluxDBConstant.createBoolean(asBooleanNotNull() == rightVal.asBooleanNotNull());
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isUInt() || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isEquals(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public InfluxDBConstant castAs(CastType type) {
            try {
                switch (type) {
                    case SIGNED:
                    case INTEGER:
                        return new InfluxDBIntConstant(value, true, true);
                    case UNSIGNED:
                        return new InfluxDBIntConstant(value, false, true);
                    case FLOAT:
                        return new InfluxDBDoubleConstant(value);
                    case STRING:
                        return InfluxDBConstant.createSingleQuotesStringConstant(String.valueOf(value));
                    case BOOLEAN:
                        return InfluxDBConstant.createBoolean(asBooleanNotNull());
                    case FIELD:
                    case TAG:
                        return this;
                    case BIGDECIMAL:
                        return InfluxDBConstant.createBigDecimalConstant(new BigDecimal(value));
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                throw new IgnoreMeException();
            }
        }

        @Override
        public String castAsString() {
            if (isSigned) {
                return String.valueOf(value);
            } else {
                return Long.toUnsignedString(value);
            }
        }

        @Override
        public InfluxDBDataType getType() {
            if (isUInt()) return InfluxDBDataType.UINT;
            else if (isBoolean()) return InfluxDBDataType.BOOLEAN;
            else return InfluxDBDataType.INT;
        }

        @Override
        public boolean isSigned() {
            return isSigned;
        }

        private String getStringRepr() {
            if (isSigned) {
                return String.valueOf(value);
            } else {
                return Long.toUnsignedString(value);
            }
        }

        @Override
        protected InfluxDBConstant isLessThan(InfluxDBConstant rightVal) {
            if (rightVal.isNull()) {
                return InfluxDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return InfluxDBConstant.createBoolean(false);
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class InfluxDBDoubleConstant extends InfluxDBConstant {
        public final static int scale = 12;
        private final double value;
        private final String stringRepresentation;

        public InfluxDBDoubleConstant(double value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
        }

        public InfluxDBDoubleConstant(double value) {
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
        public boolean asBooleanNotNull() {
            return false;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public InfluxDBConstant isEquals(InfluxDBConstant rightVal) {
            if (rightVal.isNull()) {
                return InfluxDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return InfluxDBConstant.createBoolean(false);
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isUInt() || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isEquals(rightVal);
            } else if (rightVal.isString()) {
                return castAs(CastType.STRING).isEquals(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public InfluxDBConstant castAs(CastType type) {
            try {
                switch (type) {
                    case SIGNED:
                    case INTEGER:
                        return new InfluxDBIntConstant(Math.round(value), true, true);
                    case UNSIGNED:
                        return new InfluxDBIntConstant(Math.round(value), false, true);
                    case STRING:
                        return InfluxDBConstant.createSingleQuotesStringConstant(String.valueOf(value));
                    case BOOLEAN:
                        return InfluxDBConstant.createBoolean(asBooleanNotNull());
                    case FLOAT:
                    case FIELD:
                    case TAG:
                        return this;
                    case BIGDECIMAL:
                        return InfluxDBConstant.createBigDecimalConstant(new BigDecimal(stringRepresentation));
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                throw new IgnoreMeException();
            }
        }

        @Override
        public String castAsString() {
            return String.valueOf(value);
        }

        @Override
        public InfluxDBDataType getType() {
            return InfluxDBDataType.FLOAT;
        }

        @Override
        protected InfluxDBConstant isLessThan(InfluxDBConstant rightVal) {
            if (rightVal.isNull()) {
                return InfluxDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return InfluxDBConstant.createBoolean(false);
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class InfluxDBBigDecimalConstant extends InfluxDBConstant {
        private final BigDecimal value;
        private final String stringRepresentation;

        public InfluxDBBigDecimalConstant(BigDecimal value) {
            this.value = new BigDecimal(value.toPlainString());
            this.stringRepresentation = value.toPlainString();
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
        public long getInt() {
            return value.longValue();
        }

        @Override
        public double getDouble() {
            return value.doubleValue();
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
        public boolean asBooleanNotNull() {
            return false;
        }

        @Override
        public String getTextRepresentation() {
            return stringRepresentation;
        }

        @Override
        public InfluxDBConstant isEquals(InfluxDBConstant rightVal) {
            if (rightVal.isNull()) {
                return InfluxDBConstant.createNullConstant();
            } else if (rightVal.isBoolean() || rightVal.isString()) {
                return InfluxDBConstant.createBoolean(false);
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isUInt() || rightVal.isBigDecimal()) {
                return InfluxDBConstant.createBoolean(value.subtract(
                                rightVal.castAs(CastType.BIGDECIMAL).getBigDecimalValue()).abs()
                        .compareTo(BigDecimal.valueOf(Math.pow(10, -InfluxDBDoubleConstant.scale))) <= 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public InfluxDBConstant castAs(CastType type) {
            try {
                switch (type) {
                    case SIGNED:
                    case INTEGER:
                        return new InfluxDBIntConstant(value.intValue(), true, true);
                    case UNSIGNED:
                        return new InfluxDBIntConstant(value.longValue(), false, true);
                    case STRING:
                        return InfluxDBConstant.createSingleQuotesStringConstant(stringRepresentation);
                    case BOOLEAN:
                        return InfluxDBConstant.createBoolean(asBooleanNotNull());
                    case FLOAT:
                    case FIELD:
                    case TAG:
                        return InfluxDBConstant.createDoubleConstant(value.doubleValue());
                    case BIGDECIMAL:
                        return this;
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                throw new IgnoreMeException();
            }
        }

        @Override
        public String castAsString() {
            return String.valueOf(value);
        }

        @Override
        public InfluxDBDataType getType() {
            return InfluxDBDataType.BIGDECIMAL;
        }

        @Override
        protected InfluxDBConstant isLessThan(InfluxDBConstant rightVal) {
            if (rightVal.isNull()) {
                return InfluxDBConstant.createNullConstant();
            } else if (rightVal.isBoolean() || rightVal.isString()) {
                return InfluxDBConstant.createBoolean(false);
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return InfluxDBConstant.createBoolean(value.compareTo(
                        rightVal.castAs(CastType.BIGDECIMAL).getBigDecimalValue()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class InfluxDBNullConstant extends InfluxDBConstant {

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
        public InfluxDBConstant isEquals(InfluxDBConstant rightVal) {
            return InfluxDBConstant.createNullConstant();
        }

        @Override
        public InfluxDBConstant castAs(CastType type) {
            return this;
        }

        @Override
        public String castAsString() {
            return "NULL";
        }

        @Override
        public InfluxDBDataType getType() {
            return null;
        }

        @Override
        protected InfluxDBConstant isLessThan(InfluxDBConstant rightVal) {
            return this;
        }

    }

    public long getInt() {
        throw new UnsupportedOperationException();
    }

    public double getDouble() {
        throw new UnsupportedOperationException();
    }

    public boolean isSigned() {
        return false;
    }

    public String getString() {
        throw new UnsupportedOperationException();
    }

    public boolean isString() {
        return false;
    }

    public static InfluxDBConstant createNullConstant() {
        return new InfluxDBNullConstant();
    }

    public static InfluxDBConstant createBigDecimalConstant(BigDecimal bigDecimal) {
        return new InfluxDBBigDecimalConstant(bigDecimal);
    }

    public static InfluxDBConstant createIntConstant(long value) {
        return new InfluxDBIntConstant(value, true, true);
    }

    public static InfluxDBConstant createNoSuffixIntConstant(long value) {
        return new InfluxDBIntConstant(value, true, false);
    }

    public static InfluxDBConstant createBooleanIntConstant(boolean value) {
        return new InfluxDBIntConstant(value);
    }

    public static InfluxDBConstant createIntConstant(long value, boolean signed) {
        return new InfluxDBIntConstant(value, signed, true);
    }

    public static InfluxDBConstant createIntConstant(long value, boolean signed, boolean hasSuffix) {
        return new InfluxDBIntConstant(value, signed, hasSuffix);
    }

    public static InfluxDBConstant createUnsignedIntConstant(long value) {
        return new InfluxDBIntConstant(value, false, false);
    }

    public static InfluxDBConstant createIntConstantNotAsBoolean(long value) {
        return new InfluxDBIntConstant(value, String.valueOf(value), false);
    }

    @Override
    public InfluxDBConstant getExpectedValue() {
        return this;
    }

    public abstract boolean asBooleanNotNull();

    public abstract String getTextRepresentation();

    public BigDecimal getBigDecimalValue() {
        throw new UnsupportedOperationException();
    }

    public static InfluxDBConstant createBoolean(boolean isTrue) {
        return InfluxDBConstant.createBooleanIntConstant(isTrue);
    }

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract InfluxDBConstant isEquals(InfluxDBConstant rightVal);

    public abstract InfluxDBConstant castAs(CastType type);

    public abstract String castAsString();

    public static InfluxDBConstant createSingleQuotesStringConstant(String string) {
        return new InfluxDBTextConstant(string);
    }

    public static InfluxDBConstant createDoubleConstant(double value) {
        return new InfluxDBDoubleConstant(value);
    }

    public static InfluxDBConstant createDoubleQuotesStringConstant(String string) {
        return new InfluxDBTextConstant(string, false);
    }

    public abstract InfluxDBDataType getType();

    public boolean dataTypeIsEqual(InfluxDBConstant rightVal) {
        if (getType().equals(rightVal.getType())) return true;
            // INT UINT FLOAT 在Where表达式中, 作为一种类型判断
        else if (isNumber() && rightVal.isNumber()) return true;
        else return false;
    }

    protected abstract InfluxDBConstant isLessThan(InfluxDBConstant rightVal);

    public static BigDecimal createFloatArithmeticTolerance() {
        return BigDecimal.valueOf(Math.pow(10, -InfluxDBDoubleConstant.scale));
    }
}
