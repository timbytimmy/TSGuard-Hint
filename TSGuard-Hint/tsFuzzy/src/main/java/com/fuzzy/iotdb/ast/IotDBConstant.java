package com.fuzzy.iotdb.ast;

import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.util.BigDecimalUtil;
import com.fuzzy.iotdb.IotDBSchema.IotDBDataType;
import com.fuzzy.iotdb.ast.IotDBCastOperation.CastType;
import com.fuzzy.iotdb.util.IotDBValueStateConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;

@Slf4j
public abstract class IotDBConstant implements IotDBExpression {

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

    public abstract static class IotDBNoPQSConstant extends IotDBConstant {

        @Override
        public boolean asBooleanNotNull() {
            throw throwException();
        }

        private RuntimeException throwException() {
            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
        }

        @Override
        public IotDBConstant isEquals(IotDBConstant rightVal) {
            return null;
        }

        @Override
        public IotDBConstant castAs(CastType type) {
            throw throwException();
        }

        @Override
        public String castAsString() {
            throw throwException();

        }

        @Override
        public IotDBDataType getType() {
            throw throwException();
        }

        @Override
        protected IotDBConstant isLessThan(IotDBConstant rightVal) {
            throw throwException();
        }

    }

    public static class IotDBTextConstant extends IotDBConstant {

        private final String value;
        private final boolean singleQuotes;

        public IotDBTextConstant(String value) {
            this.value = value;
            singleQuotes = false;
        }

        public IotDBTextConstant(String value, boolean singleQuotes) {
            this.value = value;
            this.singleQuotes = singleQuotes;
        }

        private void checkIfSmallFloatingPointText() {
//            boolean isSmallFloatingPointText = isString() && asBooleanNotNull()
//                    && castAs(CastType.SIGNED).getInt() == 0;
//            if (isSmallFloatingPointText) {
//                throw new IgnoreMeException();
//            }
        }

        @Override
        public boolean asBooleanNotNull() {
            return value.equalsIgnoreCase("true");
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
        public IotDBConstant isEquals(IotDBConstant rightVal) {
            if (rightVal.isNull()) {
                return IotDBConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return IotDBConstant.createBoolean(StringUtils.equals(value, rightVal.getString()));
            } else if (rightVal.isBoolean()) {
                return IotDBConstant.createBoolean(castAs(CastType.BOOLEAN).asBooleanNotNull()
                        == rightVal.asBooleanNotNull());
            } else if (rightVal.isInt() || rightVal.isBigDecimal() || rightVal.isDouble()) {
                return castAs(CastType.BIGDECIMAL).isEquals(rightVal);
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
        public IotDBConstant castAs(CastType type) {
            try {
                switch (type) {
                    case BOOLEAN:
                        if (IotDBValueStateConstant.TRUE.getValue().equalsIgnoreCase(value))
                            return IotDBConstant.createBoolean(true);
                        else if (IotDBValueStateConstant.FALSE.getValue().equalsIgnoreCase(value))
                            return IotDBConstant.createBoolean(false);
                        else {
                            log.info("不支持将该String转为Boolean值, str:{}", value);
                            throw new IgnoreMeException();
                        }
                    case INT32:
                        return IotDBConstant.createInt32Constant(Integer.parseInt(value));
                    case INT64:
                        return IotDBConstant.createInt64Constant(Long.parseLong(value));
                    case TEXT:
                        return this;
                    case FLOAT:
                    case DOUBLE:
                        return IotDBConstant.createDoubleConstant(new BigDecimal(value).doubleValue());
                    case BIGDECIMAL:
                        return IotDBConstant.createBigDecimalConstant(new BigDecimal(value));
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                // parse text error
                throw new IgnoreMeException();
            }
        }

        @Override
        public String castAsString() {
            return value;
        }

        @Override
        public IotDBDataType getType() {
            return IotDBDataType.TEXT;
        }

        @Override
        protected IotDBConstant isLessThan(IotDBConstant rightVal) {
            if (rightVal.isNull()) {
                return IotDBConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return IotDBConstant.createBoolean(StringUtils.compare(value, rightVal.getString()) < 0);
            } else if (rightVal.isBoolean()) {
                return IotDBConstant.createBoolean(false);
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class IotDBIntConstant extends IotDBConstant {

        private final long value;
        private final String stringRepresentation;
        private final IotDBDataType dataType;

        public IotDBIntConstant(long value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
            dataType = IotDBDataType.INT32;
        }

        public IotDBIntConstant(long value) {
            this.value = value;
            this.stringRepresentation = String.valueOf(value);
            dataType = IotDBDataType.INT32;
        }

        private IotDBIntConstant(boolean booleanValue) {
            this.value = booleanValue ? 1 : 0;
            this.stringRepresentation = booleanValue ? "TRUE" : "FALSE";
            dataType = IotDBDataType.BOOLEAN;
        }

        public IotDBIntConstant(long value, IotDBDataType dataType) {
            this.value = value;
            this.stringRepresentation = String.valueOf(value);
            if (!(IotDBDataType.INT32.equals(dataType)
                    || IotDBDataType.INT64.equals(dataType)
                    || IotDBDataType.BOOLEAN.equals(dataType))) {
                throw new UnsupportedOperationException(String.format("IotDBIntConstant不支持该数据类型:%s!", dataType));
            }
            this.dataType = dataType;
        }

        @Override
        public boolean isInt() {
            return IotDBDataType.INT32.equals(dataType) || IotDBDataType.INT64.equals(dataType);
        }

        @Override
        public boolean isNumber() {
            return !isBoolean();
        }

        @Override
        public boolean isBoolean() {
            return IotDBDataType.BOOLEAN.equals(dataType);
        }

        @Override
        public boolean isSigned() {
            return true;
        }

        @Override
        public long getInt() {
            switch (dataType) {
                case BOOLEAN:
                case INT32:
                    return (int) value;
                case INT64:
                    return value;
                default:
                    throw new UnsupportedOperationException(String.format("IotDBIntConstant不支持该数据类型:%s!", dataType));
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
        public IotDBConstant isEquals(IotDBConstant rightVal) {
            if (rightVal.isNull()) {
                return IotDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return IotDBConstant.createBoolean(asBooleanNotNull() == rightVal.asBooleanNotNull());
            } else if (rightVal.isString()) {
                return castAs(CastType.TEXT).isEquals(rightVal);
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isEquals(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public IotDBConstant castAs(CastType type) {
            try {
                switch (type) {
                    case BOOLEAN:
                        return IotDBConstant.createBoolean(value != 0);
                    case INT32:
                        if (IotDBDataType.INT64.equals(this.dataType)) {
                            log.warn("INT64不支持转换为INT32");
                            throw new IgnoreMeException();
                        }
                        return IotDBConstant.createInt32Constant(value);
                    case INT64:
                        return IotDBConstant.createInt64Constant(value);
                    case TEXT:
                        return IotDBConstant.createStringConstant(String.valueOf(value));
                    case FLOAT:
                    case DOUBLE:
                        return IotDBConstant.createDoubleConstant(value);
                    case BIGDECIMAL:
                        return IotDBConstant.createBigDecimalConstant(new BigDecimal(stringRepresentation));
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
        public IotDBDataType getType() {
            return this.dataType;
        }

        @Override
        protected IotDBConstant isLessThan(IotDBConstant rightVal) {
            if (rightVal.isNull()) {
                return IotDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return IotDBConstant.createBoolean(false);
            } else if (rightVal.isString()) {
                return castAs(CastType.TEXT).isLessThan(rightVal);
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class IotDBDoubleConstant extends IotDBConstant {
        // FLOAT支持精度为7位, 设置6可测试DOUBLE和FLOAT之间转换
        public final static int scale = 7;
        private final double value;
        private final String stringRepresentation;

        public IotDBDoubleConstant(double value) {
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
        public IotDBConstant isEquals(IotDBConstant rightVal) {
            if (rightVal.isNull()) {
                return IotDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return IotDBConstant.createBoolean(false);
            } else if (rightVal.isString()) {
                return castAs(CastType.TEXT).isEquals(rightVal);
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isEquals(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public IotDBConstant castAs(IotDBCastOperation.CastType type) {
            try {
                switch (type) {
                    case INT32:
                        return IotDBConstant.createInt32Constant(Math.round(value));
                    case INT64:
                        return IotDBConstant.createInt64Constant(Math.round(value));
                    case TEXT:
                        return IotDBConstant.createStringConstant(String.valueOf(value));
                    case BOOLEAN:
                        return IotDBConstant.createBoolean(
                                castAs(CastType.BIGDECIMAL).getBigDecimalValue()
                                        .compareTo(new BigDecimal(0)) != 0);
                    case FLOAT:
                    case DOUBLE:
                        return this;
                    case BIGDECIMAL:
                        return IotDBConstant.createBigDecimalConstant(new BigDecimal(stringRepresentation));
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
        public IotDBDataType getType() {
            return IotDBDataType.DOUBLE;
        }

        @Override
        protected IotDBConstant isLessThan(IotDBConstant rightVal) {
            if (rightVal.isNull()) {
                return IotDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return IotDBConstant.createBoolean(false);
            } else if (rightVal.isString()) {
                return castAs(CastType.TEXT).isLessThan(rightVal);
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return castAs(CastType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class IotDBBigDecimalConstant extends IotDBConstant {
        private final BigDecimal value;
        private final String stringRepresentation;

        public IotDBBigDecimalConstant(BigDecimal value) {
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
        public double getDouble() {
            return value.doubleValue();
        }

        @Override
        public long getInt() {
            return value.longValue();
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
        public IotDBConstant isEquals(IotDBConstant rightVal) {
            if (rightVal.isNull()) {
                return IotDBConstant.createNullConstant();
            } else if (rightVal.isBoolean() || rightVal.isString()) {
                return IotDBConstant.createBoolean(false);
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return IotDBConstant.createBoolean(value.subtract(
                                rightVal.castAs(CastType.BIGDECIMAL).getBigDecimalValue()).abs()
                        .compareTo(BigDecimal.valueOf(Math.pow(10, -IotDBDoubleConstant.scale))) <= 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public IotDBConstant castAs(IotDBCastOperation.CastType type) {
            try {
                switch (type) {
                    case INT32:
                        return IotDBConstant.createInt32Constant(value.intValue());
                    case INT64:
                        return IotDBConstant.createInt64Constant(value.longValue());
                    case TEXT:
                        return IotDBConstant.createStringConstant(value.toPlainString());
                    case BOOLEAN:
                        return IotDBConstant.createBoolean(value.compareTo(new BigDecimal(0)) != 0);
                    case FLOAT:
                    case DOUBLE:
                        return IotDBConstant.createDoubleConstant(value.doubleValue());
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
            return stringRepresentation;
        }

        @Override
        public IotDBDataType getType() {
            return IotDBDataType.BIGDECIMAL;
        }

        @Override
        protected IotDBConstant isLessThan(IotDBConstant rightVal) {
            if (rightVal.isNull()) {
                return IotDBConstant.createNullConstant();
            } else if (rightVal.isBoolean() || rightVal.isString()) {
                return IotDBConstant.createBoolean(false);
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isBigDecimal()) {
                return IotDBConstant.createBoolean(value.compareTo(
                        rightVal.castAs(CastType.BIGDECIMAL).getBigDecimalValue()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class IotDBNullConstant extends IotDBConstant {

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
        public IotDBConstant isEquals(IotDBConstant rightVal) {
            return IotDBConstant.createNullConstant();
        }

        @Override
        public IotDBConstant castAs(CastType type) {
            return this;
        }

        @Override
        public String castAsString() {
            return "NULL";
        }

        @Override
        public IotDBDataType getType() {
            return IotDBDataType.NULL;
        }

        @Override
        protected IotDBConstant isLessThan(IotDBConstant rightVal) {
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

    public static IotDBConstant createNullConstant() {
        // TODO 空值问题
        return new IotDBNullConstant();
    }

    public static IotDBConstant createDoubleConstant(double value) {
        return new IotDBDoubleConstant(value);
    }

    public static IotDBConstant createIntConstant(long value) {
        return new IotDBIntConstant(value);
    }

    public static IotDBConstant createInt32Constant(long value) {
        return new IotDBIntConstant(value, IotDBDataType.INT32);
    }

    public static IotDBConstant createBigDecimalConstant(BigDecimal bigDecimal) {
        return new IotDBBigDecimalConstant(bigDecimal);
    }

    public static IotDBConstant createInt64Constant(long value) {
        return new IotDBIntConstant(value, IotDBDataType.INT64);
    }

    public static IotDBConstant createIntConstantNotAsBoolean(long value) {
        return new IotDBIntConstant(value, String.valueOf(value));
    }

    @Override
    public IotDBConstant getExpectedValue() {
        return this;
    }

    public abstract boolean asBooleanNotNull();

    public abstract String getTextRepresentation();

    public String getTextRepresentationNoSuffix() {
        throw new AssertionError("This type does not support this method: getTextRepresentationNoSuffix");
    }

    public static IotDBConstant createFalse() {
        return IotDBConstant.createBoolean(false);
    }

    public static IotDBConstant createBoolean(boolean isTrue) {
        return new IotDBIntConstant(isTrue);
    }

    public static IotDBConstant createTrue() {
        return IotDBConstant.createBoolean(true);
    }

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract IotDBConstant isEquals(IotDBConstant rightVal);

    public abstract IotDBConstant castAs(CastType type);

    public abstract String castAsString();

    public static IotDBConstant createStringConstant(String string) {
        return new IotDBTextConstant(string);
    }

    public static IotDBConstant createStringConstant(String string, boolean singleQuotes) {
        return new IotDBTextConstant(string, singleQuotes);
    }

    public abstract IotDBDataType getType();

    protected abstract IotDBConstant isLessThan(IotDBConstant rightVal);

    public static BigDecimal createFloatArithmeticTolerance() {
        return BigDecimal.valueOf(Math.pow(10, -IotDBDoubleConstant.scale));
    }

    public static boolean dataTypeIsEqual(IotDBConstant left, IotDBConstant right) {
        if (left.isNumber() && right.isNumber()
                || left.isString() && right.isString()
                || left.isBoolean() && right.isBoolean()
                || left.isNull() && right.isNull())
            return true;
        return false;
    }
}
