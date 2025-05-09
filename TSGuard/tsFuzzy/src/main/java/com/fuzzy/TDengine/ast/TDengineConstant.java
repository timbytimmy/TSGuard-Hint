package com.fuzzy.TDengine.ast;

import com.fuzzy.IgnoreMeException;
import com.fuzzy.TDengine.TDengineSchema;
import com.fuzzy.TDengine.TDengineSchema.TDengineDataType;
import com.fuzzy.TDengine.ast.TDengineCastOperation.CastType;
import com.fuzzy.common.util.BigDecimalUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;

@Slf4j
public abstract class TDengineConstant implements TDengineExpression {

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

    public abstract static class TDengineNoPQSConstant extends TDengineConstant {

        @Override
        public boolean asBooleanNotNull() {
            throw throwException();
        }

        private RuntimeException throwException() {
            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
        }

        @Override
        public TDengineConstant isEquals(TDengineConstant rightVal) {
            return null;
        }

        @Override
        public TDengineConstant castAs(CastType type) {
            throw throwException();
        }

        @Override
        public String castAsString() {
            throw throwException();

        }

        @Override
        public TDengineDataType getType() {
            throw throwException();
        }

        @Override
        protected TDengineConstant isLessThan(TDengineConstant rightVal) {
            throw throwException();
        }

    }

    public static class TDengineTextConstant extends TDengineConstant {

        private final String value;
        private final boolean singleQuotes;

        public TDengineTextConstant(String value) {
            this.value = value;
            singleQuotes = false;
        }

        public TDengineTextConstant(String value, boolean singleQuotes) {
            this.value = value;
            this.singleQuotes = singleQuotes;
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
        public String getString() {
            return value;
        }

        @Override
        public boolean isString() {
            return true;
        }

        @Override
        public String castAsString() {
            return value;
        }

        @Override
        public TDengineDataType getType() {
            return TDengineDataType.BINARY;
        }

        @Override
        public TDengineConstant isEquals(TDengineConstant rightVal) {
            if (rightVal.isNull()) {
                return TDengineConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return TDengineConstant.createBoolean(StringUtils.equals(value, rightVal.getString()));
            } else if (rightVal.isInt() || rightVal.isDouble() || rightVal.isBoolean() || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isEquals(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public TDengineConstant castAs(CastType type) {
            try {
                switch (type) {
                    case BOOL:
                        if (value.equalsIgnoreCase("true"))
                            return new TDengineIntConstant(true);
                        else if (value.equalsIgnoreCase("false"))
                            return new TDengineIntConstant(false);
                        else {
                            log.info("不支持将该String转为Boolean值, str:{}", value);
                            throw new IgnoreMeException();
                        }
                    case INT:
                        return new TDengineIntConstant(Integer.parseInt(value), TDengineDataType.INT);
                    case UINT:
                        return new TDengineIntConstant(Integer.parseInt(value), TDengineDataType.UINT);
                    case BIGINT:
                        return new TDengineIntConstant(Long.parseLong(value), TDengineDataType.BIGINT);
                    case UBIGINT:
                        return new TDengineIntConstant(Long.parseLong(value), TDengineDataType.UBIGINT);
                    case BINARY:
                    case VARCHAR:
                        return this;
                    case FLOAT:
                    case DOUBLE:
                        return TDengineConstant.createDoubleConstant(Double.parseDouble(value));
                    case BIGDECIMAL:
                        return TDengineConstant.createBigDecimalConstant(new BigDecimal(value));
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                log.warn("parse text to number error");
                throw new IgnoreMeException();
            }
        }

        @Override
        protected TDengineConstant isLessThan(TDengineConstant rightVal) {
            if (rightVal.isNull()) {
                return TDengineConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return TDengineConstant.createBoolean(StringUtils.compare(value, rightVal.getString()) < 0);
            } else if (rightVal.isInt() || rightVal.isBoolean() || rightVal.isDouble() || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class TDengineIntConstant extends TDengineConstant {

        private final long value;
        private final String stringRepresentation;
        private final TDengineDataType dataType;
        private final boolean isSigned;

        public TDengineIntConstant(long value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
            dataType = TDengineDataType.INT;
            isSigned = true;
        }

        public TDengineIntConstant(long value, boolean isSigned) {
            this.value = value;
            this.isSigned = isSigned;
            if (isSigned) {
                dataType = TDengineDataType.INT;
                this.stringRepresentation = String.valueOf(value);
            } else {
                dataType = TDengineDataType.UINT;
                this.stringRepresentation = Long.toUnsignedString(value);
            }
        }

        public TDengineIntConstant(boolean booleanValue) {
            this.value = booleanValue ? 1 : 0;
            this.stringRepresentation = booleanValue ? "TRUE" : "FALSE";
            this.dataType = TDengineDataType.BOOL;
            this.isSigned = true;
        }

        public TDengineIntConstant(long value, TDengineDataType dataType) {
            this.value = value;
            this.stringRepresentation = String.valueOf(value);
            if (!TDengineDataType.INT.equals(dataType)
                    && !TDengineDataType.UINT.equals(dataType)
                    && !TDengineDataType.BIGINT.equals(dataType)
                    && !TDengineDataType.UBIGINT.equals(dataType)) {
                throw new UnsupportedOperationException(String.format("TDengineIntConstant不支持该数据类型:%s!", dataType));
            }
            if (TDengineDataType.UINT.equals(dataType) ||
                    TDengineDataType.UBIGINT.equals(dataType)) this.isSigned = false;
            else this.isSigned = true;
            this.dataType = dataType;
        }

        @Override
        public boolean isInt() {
            return TDengineDataType.INT.equals(dataType)
                    || TDengineDataType.UINT.equals(dataType)
                    || TDengineDataType.BIGINT.equals(dataType)
                    || TDengineDataType.UBIGINT.equals(dataType);
        }

        @Override
        public boolean isNumber() {
            return isInt();
        }

        @Override
        public boolean isBoolean() {
            return TDengineDataType.BOOL.equals(dataType);
        }

        @Override
        public long getInt() {
            switch (dataType) {
                case BOOL:
                case INT:
                case UINT:
                case BIGINT:
                case UBIGINT:
                    return value;
                default:
                    throw new UnsupportedOperationException(String.format("TDengineIntConstant不支持该数据类型:%s!", dataType));
            }
        }

        @Override
        public boolean asBooleanNotNull() {
            return isBoolean() && this.value == 1;
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
        public TDengineDataType getType() {
            return this.dataType;
        }

        private String getStringRepr() {
            return String.valueOf(value);
        }

        @Override
        public TDengineConstant isEquals(TDengineConstant rightVal) {
            if (rightVal.isNull()) {
                return TDengineConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return TDengineConstant.createBoolean(asBooleanNotNull() == rightVal.asBooleanNotNull());
            } else if (rightVal.isInt() || rightVal.isString() || rightVal.isDouble() || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isEquals(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public TDengineConstant castAs(CastType type) {
            try {
                switch (type) {
                    case BOOL:
                        return TDengineConstant.createBoolean(value != 0);
                    case INT:
                        if (TDengineDataType.BIGINT.equals(this.dataType) || TDengineDataType.UBIGINT.equals(this.dataType))
                            throw new UnsupportedOperationException("INT64不支持转换为INT32");
                        return new TDengineIntConstant((int) value, TDengineDataType.INT);
                    case UINT:
                        if (TDengineDataType.BIGINT.equals(this.dataType) || TDengineDataType.UBIGINT.equals(this.dataType))
                            throw new UnsupportedOperationException("INT64不支持转换为INT32");
                        return new TDengineIntConstant(Long.parseLong(Long.toUnsignedString(value)), TDengineDataType.UINT);
                    case BIGINT:
                        return new TDengineIntConstant(value, TDengineDataType.BIGINT);
                    case UBIGINT:
                        return new TDengineIntConstant(Long.parseLong(Long.toUnsignedString(value)), TDengineDataType.UBIGINT);
                    case BINARY:
                    case VARCHAR:
                        return new TDengineTextConstant(String.valueOf(value));
                    case FLOAT:
                    case DOUBLE:
                        return TDengineConstant.createDoubleConstant(value);
                    case BIGDECIMAL:
                        return TDengineConstant.createBigDecimalConstant(new BigDecimal(value));
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                log.warn("数字转换格式错误");
                throw new IgnoreMeException();
            }
        }

        @Override
        protected TDengineConstant isLessThan(TDengineConstant rightVal) {
            if (rightVal.isNull()) {
                return TDengineIntConstant.createNullConstant();
            } else if (rightVal.isInt() || rightVal.isString() || rightVal.isDouble() || rightVal.isBoolean()
                    || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class TDengineDoubleConstant extends TDengineConstant {
        public final static int scale = 7;
        private final double value;
        private final String stringRepresentation;

        public TDengineDoubleConstant(double value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
        }

        public TDengineDoubleConstant(double value) {
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
        public TDengineSchema.TDengineDataType getType() {
            return TDengineSchema.TDengineDataType.DOUBLE;
        }

        @Override
        public TDengineConstant isEquals(TDengineConstant rightVal) {
            if (rightVal.isNull()) {
                return TDengineConstant.createNullConstant();
            } else if (rightVal.isDouble() || rightVal.isBoolean() || rightVal.isInt() || rightVal.isBigDecimal() ||
                    rightVal.isString()) {
                return castAs(CastType.BIGDECIMAL).isEquals(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public TDengineConstant castAs(TDengineCastOperation.CastType type) {
            try {
                switch (type) {
                    case INT:
                        return TDengineConstant.createInt32Constant((int) value);
                    case UINT:
                        throw new AssertionError();
                    case BIGINT:
                        return TDengineConstant.createInt64Constant((long) value);
                    case UBIGINT:
                        throw new AssertionError();
                    case BINARY:
                    case VARCHAR:
                        return TDengineConstant.createStringConstant(BigDecimal.valueOf(value)
                                .setScale(TDengineDoubleConstant.scale, RoundingMode.HALF_UP).toPlainString());
                    case BOOL:
                        return TDengineConstant.createBoolean(castAs(CastType.BIGDECIMAL).getBigDecimalValue()
                                .compareTo(new BigDecimal(0)) != 0);
                    case FLOAT:
                    case DOUBLE:
                        return this;
                    case BIGDECIMAL:
                        return TDengineConstant.createBigDecimalConstant(new BigDecimal(stringRepresentation));
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                log.warn("数字转换格式错误");
                throw new IgnoreMeException();
            }
        }

        @Override
        protected TDengineConstant isLessThan(TDengineConstant rightVal) {
            if (rightVal.isNull()) {
                return TDengineConstant.createNullConstant();
            } else if (rightVal.isDouble() || rightVal.isBoolean() || rightVal.isInt() || rightVal.isBigDecimal() ||
                    rightVal.isString()) {
                return castAs(CastType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class TDengineBigDecimalConstant extends TDengineConstant {
        private final BigDecimal value;
        private final String stringRepresentation;

        public TDengineBigDecimalConstant(BigDecimal value) {
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
        public TDengineSchema.TDengineDataType getType() {
            return TDengineSchema.TDengineDataType.BIGDECIMAL;
        }

        @Override
        public TDengineConstant isEquals(TDengineConstant rightVal) {
            if (rightVal.isNull()) {
                return TDengineConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return TDengineConstant.createFalse();
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isString() || rightVal.isBigDecimal()) {
                return TDengineConstant.createBoolean(value.subtract(
                                rightVal.castAs(CastType.BIGDECIMAL).getBigDecimalValue())
                        .abs().compareTo(BigDecimal.valueOf(Math.pow(10, -TDengineDoubleConstant.scale))) <= 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public TDengineConstant castAs(TDengineCastOperation.CastType type) {
            try {
                switch (type) {
                    case INT:
                        return TDengineConstant.createInt32Constant(value.intValue());
                    case UINT:
                        return TDengineConstant.createUInt32Constant(value.intValue());
                    case BIGINT:
                        return TDengineConstant.createInt64Constant(value.longValue());
                    case UBIGINT:
                        return TDengineConstant.createUInt64Constant(value.longValue());
                    case BINARY:
                    case VARCHAR:
                        return TDengineConstant.createStringConstant(value.setScale(TDengineDoubleConstant.scale,
                                RoundingMode.HALF_DOWN).toPlainString());
                    case BOOL:
                        return TDengineConstant.createBoolean(value.compareTo(new BigDecimal(0)) != 0);
                    case FLOAT:
                    case DOUBLE:
                        return TDengineConstant.createDoubleConstant(value.doubleValue());
                    case BIGDECIMAL:
                        return this;
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                log.warn("数字转换格式错误");
                throw new IgnoreMeException();
            }
        }

        @Override
        protected TDengineConstant isLessThan(TDengineConstant rightVal) {
            if (rightVal.isNull()) {
                return TDengineConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return TDengineConstant.createFalse();
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isString() || rightVal.isBigDecimal()) {
                return TDengineConstant.createBoolean(value.compareTo(
                        rightVal.castAs(CastType.BIGDECIMAL).getBigDecimalValue()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class TDengineNullConstant extends TDengineConstant {

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
        public TDengineConstant isEquals(TDengineConstant rightVal) {
            return TDengineConstant.createNullConstant();
        }

        @Override
        public TDengineConstant castAs(CastType type) {
            return this;
        }

        @Override
        public String castAsString() {
            return "NULL";
        }

        @Override
        public TDengineDataType getType() {
            return TDengineDataType.NULL;
        }

        @Override
        protected TDengineConstant isLessThan(TDengineConstant rightVal) {
            return this;
        }

    }

    public long getInt() {
        throw new UnsupportedOperationException();
    }

    public double getDouble() {
        throw new UnsupportedOperationException();
    }

    public BigDecimal getBigDecimalValue() {
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

    public boolean isNumber() {
        return false;
    }

    public static TDengineConstant createNullConstant() {
        return new TDengineNullConstant();
    }

    public static TDengineConstant createBooleanIntConstant(boolean value) {
        return new TDengineIntConstant(value);
    }

    public static TDengineConstant createInt32Constant(long value) {
        return new TDengineIntConstant(value, TDengineDataType.INT);
    }

    public static TDengineConstant createUInt32Constant(long value) {
        return new TDengineIntConstant(value, TDengineDataType.UINT);
    }

    public static TDengineConstant createInt64Constant(long value) {
        return new TDengineIntConstant(value, TDengineDataType.BIGINT);
    }

    public static TDengineConstant createBigDecimalConstant(BigDecimal value) {
        return new TDengineBigDecimalConstant(value);
    }

    public static TDengineConstant createUInt64Constant(long value) {
        return new TDengineIntConstant(value, TDengineDataType.UBIGINT);
    }

    public static TDengineConstant createDoubleConstant(double value) {
        return new TDengineDoubleConstant(value);
    }

    public static TDengineConstant createIntConstantNotAsBoolean(long value) {
        return new TDengineIntConstant(value, String.valueOf(value));
    }

    @Override
    public TDengineConstant getExpectedValue() {
        return this;
    }

    public abstract boolean asBooleanNotNull();

    public abstract String getTextRepresentation();

    public String getTextRepresentationNoSuffix() {
        throw new AssertionError("This type does not support this method: getTextRepresentationNoSuffix");
    }

    public static TDengineConstant createFalse() {
        return TDengineConstant.createBooleanIntConstant(false);
    }

    public static TDengineConstant createBoolean(boolean isTrue) {
        return TDengineConstant.createBooleanIntConstant(isTrue);
    }

    public static TDengineConstant createTrue() {
        return TDengineConstant.createBooleanIntConstant(true);
    }

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract TDengineConstant isEquals(TDengineConstant rightVal);

    public abstract TDengineConstant castAs(CastType type);

    public abstract String castAsString();

    public static TDengineConstant createStringConstant(String string) {
        return new TDengineTextConstant(string);
    }

    public static TDengineConstant createStringConstant(String string, boolean singleQuotes) {
        return new TDengineTextConstant(string, singleQuotes);
    }

    public abstract TDengineDataType getType();

    protected abstract TDengineConstant isLessThan(TDengineConstant rightVal);

    public static BigDecimal createFloatArithmeticTolerance() {
        return BigDecimal.valueOf(Math.pow(10, -TDengineDoubleConstant.scale));
    }

    public static boolean dataTypeIsEqual(TDengineConstant left, TDengineConstant right) {
        if (left.isNumber() && right.isNumber()
                || left.isString() && right.isString()
                || left.isBoolean() && right.isBoolean()
                || left.isNull() && right.isNull())
            return true;
        return false;
    }
}
