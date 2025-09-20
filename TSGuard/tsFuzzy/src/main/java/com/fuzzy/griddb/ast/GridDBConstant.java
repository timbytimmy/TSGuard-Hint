package com.fuzzy.griddb.ast;

import cn.hutool.core.util.ObjectUtil;
import com.fuzzy.IgnoreMeException;
import com.fuzzy.common.util.BigDecimalUtil;
import com.fuzzy.griddb.GridDBSchema.GridDBDataType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;

@Slf4j
public abstract class GridDBConstant implements GridDBExpression {

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

    public boolean isTimestamp() {
        return false;
    }

    public abstract static class GridDBNoPQSConstant extends GridDBConstant {

        @Override
        public boolean asBooleanNotNull() {
            throw throwException();
        }

        private RuntimeException throwException() {
            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
        }

        @Override
        public GridDBConstant isEquals(GridDBConstant rightVal) {
            return null;
        }

        @Override
        public GridDBConstant castAs(GridDBDataType type) {
            throw throwException();
        }

        @Override
        public String castAsString() {
            throw throwException();

        }

        @Override
        public GridDBDataType getType() {
            throw throwException();
        }

        @Override
        protected GridDBConstant isLessThan(GridDBConstant rightVal) {
            throw throwException();
        }

        @Override
        public String getTextRepresentation() {
            return "NULL";
        }

    }

    public static class GridDBTextConstant extends GridDBConstant {

        private final String value;
        private final boolean singleQuotes;

        public GridDBTextConstant(String value) {
            this.value = value;
            singleQuotes = true;
        }

        public GridDBTextConstant(String value, boolean singleQuotes) {
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
        public GridDBDataType getType() {
            return GridDBDataType.STRING;
        }

        @Override
        public GridDBConstant isEquals(GridDBConstant rightVal) {
            if (rightVal.isNull()) {
                return GridDBConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return GridDBConstant.createBoolean(StringUtils.equals(value, rightVal.getString()));
            } else if (rightVal.isInt() || rightVal.isDouble() || rightVal.isBoolean() || rightVal.isBigDecimal()) {
                return castAs(GridDBDataType.BIGDECIMAL).isEquals(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public GridDBConstant castAs(GridDBDataType type) {
            try {
                switch (type) {
                    case BOOL:
                        if (value.equalsIgnoreCase("true"))
                            return new GridDBIntConstant(true);
                        else if (value.equalsIgnoreCase("false"))
                            return new GridDBIntConstant(false);
                        else {
                            log.info("不支持将该String转为Boolean值, str:{}", value);
                            throw new IgnoreMeException();
                        }
                    case INTEGER:
                        return new GridDBIntConstant(Integer.parseInt(value), GridDBDataType.INTEGER);
                    case LONG:
                        return new GridDBIntConstant(Long.parseLong(value), GridDBDataType.LONG);
                    case STRING:
                        return this;
                    case FLOAT:
                    case DOUBLE:
                        return GridDBConstant.createDoubleConstant(Double.parseDouble(value));
                    case BIGDECIMAL:
                        return GridDBConstant.createBigDecimalConstant(new BigDecimal(value));
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                log.warn("字符串转数值异常, GridDBTextConstant cast as");
                throw new IgnoreMeException();
            }
        }

        @Override
        protected GridDBConstant isLessThan(GridDBConstant rightVal) {
            if (rightVal.isNull()) {
                return GridDBConstant.createNullConstant();
            } else if (rightVal.isString()) {
                return GridDBConstant.createBoolean(StringUtils.compare(value, rightVal.getString()) < 0);
            } else if (rightVal.isInt() || rightVal.isBoolean() || rightVal.isDouble() || rightVal.isBigDecimal()) {
                return castAs(GridDBDataType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class GridDBIntConstant extends GridDBConstant {

        private final long value;
        private final String stringRepresentation;
        private final GridDBDataType dataType;

        public GridDBIntConstant(boolean booleanValue) {
            this.value = booleanValue ? 1 : 0;
            this.stringRepresentation = booleanValue ? "TRUE" : "FALSE";
            this.dataType = GridDBDataType.BOOL;
        }

        public GridDBIntConstant(long value, GridDBDataType dataType) {
            this.value = value;
            this.stringRepresentation = String.valueOf(value);
            if (!GridDBDataType.INTEGER.equals(dataType)
                    && !GridDBDataType.SHORT.equals(dataType)
                    && !GridDBDataType.BYTE.equals(dataType)
                    && !GridDBDataType.LONG.equals(dataType)
                    && !GridDBDataType.BOOL.equals(dataType)
                    && !GridDBDataType.TIMESTAMP.equals(dataType)) {
                throw new UnsupportedOperationException(String.format("GridDBIntConstant不支持该数据类型:%s!", dataType));
            }
            this.dataType = dataType;
        }

        @Override
        public boolean isInt() {
            return !GridDBDataType.BOOL.equals(dataType);
        }

        public boolean isTimestamp() {
            return ObjectUtil.equals(GridDBDataType.TIMESTAMP, dataType);
        }

        @Override
        public boolean isNumber() {
            return isInt() || isTimestamp();
        }

        @Override
        public boolean isBoolean() {
            return GridDBDataType.BOOL.equals(dataType);
        }

        @Override
        public long getInt() {
            switch (dataType) {
                case BOOL:
                case INTEGER:
                case LONG:
                    return value;
                default:
                    throw new UnsupportedOperationException(String.format("GridDBIntConstant不支持该数据类型:%s!", dataType));
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
        public GridDBDataType getType() {
            return this.dataType;
        }

        private String getStringRepr() {
            return String.valueOf(value);
        }

        @Override
        public GridDBConstant isEquals(GridDBConstant rightVal) {
            if (rightVal.isNull()) {
                return GridDBConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return GridDBConstant.createBoolean(asBooleanNotNull() == rightVal.asBooleanNotNull());
            } else if (rightVal.isInt() || rightVal.isString() || rightVal.isDouble() || rightVal.isBigDecimal()) {
                return castAs(GridDBDataType.BIGDECIMAL).isEquals(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public GridDBConstant castAs(GridDBDataType type) {
            try {
                switch (type) {
                    case BOOL:
                        return GridDBConstant.createBoolean(value != 0);
                    case INTEGER:
                        if (GridDBDataType.LONG.equals(this.dataType))
                            throw new UnsupportedOperationException("INT64不支持转换为INT32");
                        return new GridDBIntConstant((int) value, GridDBDataType.INTEGER);
                    case LONG:
                        return new GridDBIntConstant(value, GridDBDataType.LONG);
                    case STRING:
                        return new GridDBTextConstant(String.valueOf(value));
                    case FLOAT:
                    case DOUBLE:
                        return GridDBConstant.createDoubleConstant(value);
                    case BIGDECIMAL:
                        return GridDBConstant.createBigDecimalConstant(new BigDecimal(value));
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                log.warn("整型cast异常");
                throw new IgnoreMeException();
            }
        }

        @Override
        protected GridDBConstant isLessThan(GridDBConstant rightVal) {
            if (rightVal.isNull()) {
                return GridDBIntConstant.createNullConstant();
            } else if (rightVal.isInt() || rightVal.isString() || rightVal.isDouble() || rightVal.isBoolean()
                    || rightVal.isBigDecimal()) {
                return castAs(GridDBDataType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class GridDBDoubleConstant extends GridDBConstant {
        public final static int scale = 6;
        private final double value;
        private final String stringRepresentation;

        public GridDBDoubleConstant(double value, String stringRepresentation) {
            this.value = value;
            this.stringRepresentation = stringRepresentation;
        }

        public GridDBDoubleConstant(double value) {
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
        public GridDBDataType getType() {
            return GridDBDataType.DOUBLE;
        }

        @Override
        public GridDBConstant isEquals(GridDBConstant rightVal) {
            if (rightVal.isNull()) {
                return GridDBConstant.createNullConstant();
            } else if (rightVal.isDouble() || rightVal.isBoolean() || rightVal.isInt() || rightVal.isBigDecimal() ||
                    rightVal.isString()) {
                return castAs(GridDBDataType.BIGDECIMAL).isEquals(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public GridDBConstant castAs(GridDBDataType type) {
            try {
                switch (type) {
                    case INTEGER:
                        return GridDBConstant.createInt32Constant((int) value);
                    case LONG:
                        return GridDBConstant.createInt64Constant((long) value);
                    case STRING:
                        return GridDBConstant.createStringConstant(String.valueOf(value));
                    case BOOL:
                        return GridDBConstant.createBoolean(castAs(GridDBDataType.BIGDECIMAL).getBigDecimalValue()
                                .compareTo(new BigDecimal(0)) != 0);
                    case FLOAT:
                    case DOUBLE:
                        return this;
                    case BIGDECIMAL:
                        return GridDBConstant.createBigDecimalConstant(new BigDecimal(stringRepresentation));
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                log.warn("double cast 异常");
                throw new IgnoreMeException();
            }
        }

        @Override
        protected GridDBConstant isLessThan(GridDBConstant rightVal) {
            if (rightVal.isNull()) {
                return GridDBConstant.createNullConstant();
            } else if (rightVal.isDouble() || rightVal.isBoolean() || rightVal.isInt() || rightVal.isBigDecimal() ||
                    rightVal.isString()) {
                return castAs(GridDBDataType.BIGDECIMAL).isLessThan(rightVal);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class GridDBBigDecimalConstant extends GridDBConstant {
        private final BigDecimal value;
        private final String stringRepresentation;

        public GridDBBigDecimalConstant(BigDecimal value) {
            this.value = new BigDecimal(value.toPlainString());
            if (isInt()) this.stringRepresentation = value.stripTrailingZeros().toPlainString();
            else this.stringRepresentation = value.toPlainString();
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
        public boolean isBigDecimal() {
            return true;
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
        public GridDBDataType getType() {
            return GridDBDataType.BIGDECIMAL;
        }

        @Override
        public GridDBConstant isEquals(GridDBConstant rightVal) {
            if (rightVal.isNull()) {
                return GridDBConstant.createNullConstant();
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isString() || rightVal.isBoolean()
                    || rightVal.isBigDecimal()) {
                return GridDBConstant.createBoolean(value.subtract(
                                rightVal.castAs(GridDBDataType.BIGDECIMAL).getBigDecimalValue())
                        .abs().compareTo(BigDecimal.valueOf(Math.pow(10, -GridDBDoubleConstant.scale))) <= 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public GridDBConstant castAs(GridDBDataType type) {
            try {
                switch (type) {
                    case INTEGER:
                        return GridDBConstant.createInt32Constant(value.intValue());
                    case LONG:
                        return GridDBConstant.createInt64Constant(value.longValue());
                    case STRING:
                        return GridDBConstant.createStringConstant(stringRepresentation);
                    case BOOL:
                        return GridDBConstant.createBoolean(value.compareTo(new BigDecimal(0)) != 0);
                    case FLOAT:
                    case DOUBLE:
                        return GridDBConstant.createDoubleConstant(value.doubleValue());
                    case BIGDECIMAL:
                        return this;
                    default:
                        throw new AssertionError();
                }
            } catch (NumberFormatException e) {
                log.warn("bigDecimal cast 异常");
                throw new IgnoreMeException();
            }
        }

        @Override
        protected GridDBConstant isLessThan(GridDBConstant rightVal) {
            if (rightVal.isNull()) {
                return GridDBConstant.createNullConstant();
            } else if (rightVal.isDouble() || rightVal.isInt() || rightVal.isString() || rightVal.isBoolean()
                    || rightVal.isBigDecimal()) {
                return GridDBConstant.createBoolean(value.compareTo(
                        rightVal.castAs(GridDBDataType.BIGDECIMAL).getBigDecimalValue()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class GridDBNullConstant extends GridDBConstant {

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
        public GridDBConstant isEquals(GridDBConstant rightVal) {
            return GridDBConstant.createNullConstant();
        }

        @Override
        public GridDBConstant castAs(GridDBDataType type) {
            return this;
        }

        @Override
        public String castAsString() {
            return "NULL";
        }

        @Override
        public GridDBDataType getType() {
            return GridDBDataType.NULL;
        }

        @Override
        protected GridDBConstant isLessThan(GridDBConstant rightVal) {
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

    public String getString() {
        throw new UnsupportedOperationException();
    }

    public boolean isString() {
        return false;
    }

    public boolean isNumber() {
        return false;
    }

    public static GridDBConstant createNullConstant() {
        return new GridDBNullConstant();
    }

    public static GridDBConstant createBooleanIntConstant(boolean value) {
        return new GridDBIntConstant(value);
    }

    public static GridDBConstant createInt32Constant(long value) {
        return new GridDBIntConstant(value, GridDBDataType.INTEGER);
    }

    public static GridDBConstant createInt64Constant(long value) {
        return new GridDBIntConstant(value, GridDBDataType.LONG);
    }

    public static GridDBConstant createTimestamp(long value) {
        return new GridDBIntConstant(value, GridDBDataType.TIMESTAMP);
    }

    public static GridDBConstant createBigDecimalConstant(BigDecimal value) {
        return new GridDBBigDecimalConstant(value);
    }

    public static GridDBConstant createDoubleConstant(double value) {
        return new GridDBDoubleConstant(value);
    }

    @Override
    public GridDBConstant getExpectedValue() {
        return this;
    }

    public abstract boolean asBooleanNotNull();

    public abstract String getTextRepresentation();

    public static GridDBConstant createBoolean(boolean isTrue) {
        return GridDBConstant.createBooleanIntConstant(isTrue);
    }

    @Override
    public String toString() {
        return getTextRepresentation();
    }

    public abstract GridDBConstant isEquals(GridDBConstant rightVal);

    public abstract GridDBConstant castAs(GridDBDataType type);

    public abstract String castAsString();

    public static GridDBConstant createStringConstant(String string) {
        return new GridDBTextConstant(string);
    }

    public static GridDBConstant createStringConstant(String string, boolean singleQuotes) {
        return new GridDBTextConstant(string, singleQuotes);
    }

    public abstract GridDBDataType getType();

    protected abstract GridDBConstant isLessThan(GridDBConstant rightVal);

    public static BigDecimal createFloatArithmeticTolerance() {
        return BigDecimal.valueOf(Math.pow(10, -GridDBDoubleConstant.scale));
    }

    public static boolean dataTypeIsEqual(GridDBConstant left, GridDBConstant right) {
        if (left.isNumber() && right.isNumber()
                || left.isString() && right.isString()
                || left.isBoolean() && right.isBoolean()
                || left.isNull() && right.isNull())
            return true;
        return false;
    }
}
