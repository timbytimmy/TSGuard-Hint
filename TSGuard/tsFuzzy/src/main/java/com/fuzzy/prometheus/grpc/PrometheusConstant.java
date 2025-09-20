package com.fuzzy.prometheus.grpc;

import com.fuzzy.prometheus.ast.PrometheusExpression;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class PrometheusConstant implements PrometheusExpression {

//    public boolean isInt() {
//        return false;
//    }
//
//    public boolean isNull() {
//        return false;
//    }
//
//    public boolean isBoolean() {
//        return false;
//    }
//
//    public abstract static class PrometheusNoPQSConstant extends PrometheusConstant {
//
//        @Override
//        public boolean asBooleanNotNull() {
//            throw throwException();
//        }
//
//        private RuntimeException throwException() {
//            throw new UnsupportedOperationException("not applicable for PQS evaluation!");
//        }
//
//        @Override
//        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
//            return null;
//        }
//
//        @Override
//        public PrometheusConstant castAs(CastType type) {
//            throw throwException();
//        }
//
//        @Override
//        public String castAsString() {
//            throw throwException();
//
//        }
//
//        @Override
//        public PrometheusDataType getType() {
//            throw throwException();
//        }
//
//        @Override
//        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
//            throw throwException();
//        }
//
//    }
//
//    public static class PrometheusDoubleConstant extends PrometheusNoPQSConstant {
//
//        private final double val;
//
//        public PrometheusDoubleConstant(double val) {
//            this.val = val;
//            if (Double.isInfinite(val) || Double.isNaN(val)) {
//                // seems to not be supported by Prometheus
//                throw new IgnoreMeException();
//            }
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return String.valueOf(val);
//        }
//
//    }
//
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
//                PrometheusConstant castIntVal = castAs(CastType.PrometheusDataTypeToCastType(rightVal.getType()));
//                return PrometheusConstant.createBoolean(new BigInteger(((PrometheusIntConstant) castIntVal).getStringRepr())
//                        .compareTo(new BigInteger(((PrometheusIntConstant) rightVal).getStringRepr())) == 0);
//            } else if (rightVal.isBoolean()) {
//                PrometheusConstant castIntVal = castAs(CastType.PrometheusDataTypeToCastType(rightVal.getType()));
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
//                        return new PrometheusIntConstant(Long.parseLong(value), PrometheusDataType.INT64);
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
//        public PrometheusDataType getType() {
//            return PrometheusDataType.TEXT;
//        }
//
//        @Override
//        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
//            try {
//                if (PrometheusDataType.INT32.equals(rightVal.getType())) {
//                    return new PrometheusIntConstant(Integer.parseInt(value) < rightVal.getInt());
//                } else if (PrometheusDataType.INT64.equals(rightVal.getType())) {
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
//
//    public static class PrometheusIntConstant extends PrometheusConstant {
//
//        private final long value;
//        private final String stringRepresentation;
//        private final PrometheusDataType dataType;
//
//        public PrometheusIntConstant(long value, String stringRepresentation) {
//            this.value = value;
//            this.stringRepresentation = stringRepresentation;
//            dataType = PrometheusDataType.INT32;
//        }
//
//        public PrometheusIntConstant(long value) {
//            this.value = value;
//            this.stringRepresentation = String.valueOf(value);
//            dataType = PrometheusDataType.INT32;
//        }
//
//        public PrometheusIntConstant(boolean booleanValue) {
//            this.value = booleanValue ? 1 : 0;
//            this.stringRepresentation = booleanValue ? "TRUE" : "FALSE";
//            dataType = PrometheusDataType.BOOLEAN;
//        }
//
//        public PrometheusIntConstant(long value, PrometheusDataType dataType) {
//            this.value = value;
//            this.stringRepresentation = String.valueOf(value);
//            if (!PrometheusDataType.INT32.equals(dataType) && !PrometheusDataType.INT64.equals(dataType)) {
//                throw new UnsupportedOperationException(String.format("PrometheusIntConstant不支持该数据类型:%s!", dataType));
//            }
//            this.dataType = dataType;
//        }
//
//        @Override
//        public boolean isInt() {
//            return PrometheusDataType.INT32.equals(dataType) || PrometheusDataType.INT64.equals(dataType);
//        }
//
//        @Override
//        public boolean isBoolean() {
//            return PrometheusDataType.BOOLEAN.equals(dataType);
//        }
//
//        @Override
//        public long getInt() {
//            switch (dataType) {
//                case BOOLEAN:
//                case INT32:
//                    return (int) value;
//                case INT64:
//                    return value;
//                default:
//                    throw new UnsupportedOperationException(String.format("PrometheusIntConstant不支持该数据类型:%s!", dataType));
//            }
//        }
//
//        @Override
//        public boolean asBooleanNotNull() {
//            return isBoolean() && this.value != 0;
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return stringRepresentation;
//        }
//
//        @Override
//        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
//            if (rightVal.isInt()) {
//                return PrometheusConstant.createBoolean(new BigInteger(getStringRepr())
//                        .compareTo(new BigInteger(((PrometheusIntConstant) rightVal).getStringRepr())) == 0);
//            } else if (rightVal.isNull()) {
//                return PrometheusConstant.createNullConstant();
//            } else if (rightVal.isString()) {
//                return isEquals(rightVal.castAs(CastType.PrometheusDataTypeToCastType(getType())));
//            } else if (rightVal.isBoolean()) {
//                PrometheusConstant castIntVal = castAs(CastType.PrometheusDataTypeToCastType(rightVal.getType()));
//                return PrometheusConstant.createBoolean(castIntVal.asBooleanNotNull() == rightVal.asBooleanNotNull());
//            } else {
//                throw new AssertionError(rightVal);
//            }
//        }
//
//        @Override
//        public PrometheusConstant castAs(CastType type) {
//            switch (type) {
//                case BOOLEAN:
//                    return new PrometheusIntConstant(value != 0);
//                case INT32:
//                    if (PrometheusDataType.INT64.equals(this.dataType))
//                        throw new UnsupportedOperationException("INT64不支持转换为INT32");
//                    return new PrometheusIntConstant(value);
//                case INT64:
//                    return new PrometheusIntConstant(value, PrometheusDataType.INT64);
//                case TEXT:
//                    return new PrometheusTextConstant(String.valueOf(value));
//                case FLOAT:
//                case DOUBLE:
//                default:
//                    throw new AssertionError();
//            }
//        }
//
//        @Override
//        public String castAsString() {
//            return String.valueOf(value);
//        }
//
//        @Override
//        public PrometheusDataType getType() {
//            return this.dataType;
//        }
//
//        private String getStringRepr() {
//            return String.valueOf(value);
//        }
//
//        @Override
//        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
//            if (rightVal.isInt()) {
//                return new PrometheusIntConstant(value < rightVal.getInt());
//            } else if (rightVal.isString()) {
//                return new PrometheusIntConstant(value < rightVal.castAs(CastType.INT64).getInt());
//            } else {
//                // TODO float、double
//                throw new AssertionError(rightVal);
//            }
//        }
//
//    }
//
//    public static class PrometheusNullConstant extends PrometheusConstant {
//
//        @Override
//        public boolean isNull() {
//            return true;
//        }
//
//        @Override
//        public boolean asBooleanNotNull() {
//            throw new UnsupportedOperationException(this.toString());
//        }
//
//        @Override
//        public String getTextRepresentation() {
//            return "NULL";
//        }
//
//        @Override
//        public PrometheusConstant isEquals(PrometheusConstant rightVal) {
//            return PrometheusConstant.createNullConstant();
//        }
//
//        @Override
//        public PrometheusConstant castAs(CastType type) {
//            return this;
//        }
//
//        @Override
//        public String castAsString() {
//            return "NULL";
//        }
//
//        @Override
//        public PrometheusDataType getType() {
//            return PrometheusDataType.NULL;
//        }
//
//        @Override
//        protected PrometheusConstant isLessThan(PrometheusConstant rightVal) {
//            return this;
//        }
//
//    }
//
//    public long getInt() {
//        throw new UnsupportedOperationException();
//    }
//
//    public boolean isSigned() {
//        return false;
//    }
//
//    public boolean hasSuffix() {
//        return false;
//    }
//
//    public String getString() {
//        throw new UnsupportedOperationException();
//    }
//
//    public boolean isString() {
//        return false;
//    }
//
//    public static PrometheusConstant createNullConstant() {
//        // TODO 空值问题
//        return new PrometheusNullConstant();
//    }
//
//    /*public static PrometheusConstant createBooleanConstant(boolean value) {
//        return new PrometheusBooleanConstant(value);
//    }*/
//
//    public static PrometheusConstant createIntConstant(long value) {
//        return new PrometheusIntConstant(value);
//    }
//
//    public static PrometheusConstant createBooleanIntConstant(boolean value) {
//        return new PrometheusIntConstant(value);
//    }
//
//    public static PrometheusConstant createInt32Constant(long value) {
//        return new PrometheusIntConstant(value, PrometheusDataType.INT32);
//    }
//
//    public static PrometheusConstant createInt64Constant(long value) {
//        return new PrometheusIntConstant(value, PrometheusDataType.INT64);
//    }
//
//    public static PrometheusConstant createIntConstantNotAsBoolean(long value) {
//        return new PrometheusIntConstant(value, String.valueOf(value));
//    }
//
//    @Override
//    public PrometheusConstant getExpectedValue() {
//        return this;
//    }
//
//    public abstract boolean asBooleanNotNull();
//
//    public abstract String getTextRepresentation();
//
//    public String getTextRepresentationNoSuffix() {
//        throw new AssertionError("This type does not support this method: getTextRepresentationNoSuffix");
//    }
//
//    public static PrometheusConstant createFalse() {
//        return PrometheusConstant.createBooleanIntConstant(false);
//    }
//
//    public static PrometheusConstant createBoolean(boolean isTrue) {
//        return PrometheusConstant.createBooleanIntConstant(isTrue);
//    }
//
//    public static PrometheusConstant createTrue() {
//        return PrometheusConstant.createBooleanIntConstant(true);
//    }
//
//    @Override
//    public String toString() {
//        return getTextRepresentation();
//    }
//
//    public abstract PrometheusConstant isEquals(PrometheusConstant rightVal);
//
//    public abstract PrometheusConstant castAs(CastType type);
//
//    public abstract String castAsString();
//
//    public static PrometheusConstant createDoubleQuotesStringConstant(String string) {
//        return new PrometheusTextConstant(string);
//    }
//
//    public static PrometheusConstant createDoubleQuotesStringConstant(String string, boolean singleQuotes) {
//        return new PrometheusTextConstant(string, singleQuotes);
//    }
//
//    public abstract PrometheusDataType getType();
//
//    protected abstract PrometheusConstant isLessThan(PrometheusConstant rightVal);

}
