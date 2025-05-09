package com.benchmark.constants;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public enum DataType {
    UNKNOWN((byte) -1),
    BOOLEAN((byte) 0),
    INT32((byte) 1),
    INT64((byte) 2),
    FLOAT((byte) 3),
    DOUBLE((byte) 4),
    TEXT((byte) 5),
    VECTOR((byte) 6);

    private final byte type;

    private DataType(byte type) {
        this.type = type;
    }

    public static DataType deserialize(byte type) {
        return getDataType(type);
    }

    public static DataType getDataType(byte type) {
        switch (type) {
            case 0:
                return BOOLEAN;
            case 1:
                return INT32;
            case 2:
                return INT64;
            case 3:
                return FLOAT;
            case 4:
                return DOUBLE;
            case 5:
                return TEXT;
            case 6:
                return VECTOR;
            default:
                throw new IllegalArgumentException("Invalid input: " + type);
        }
    }

    public byte getType() {
        return this.type;
    }

    public static DataType deserializeFrom(ByteBuffer buffer) {
        return deserialize(buffer.get());
    }

    public static int getSerializedSize() {
        return 1;
    }

    public void serializeTo(ByteBuffer byteBuffer) {
        byteBuffer.put(this.serialize());
    }

    public void serializeTo(DataOutputStream outputStream) throws IOException {
        outputStream.write(this.serialize());
    }

    public int getDataTypeSize() {
        switch (this) {
            case BOOLEAN:
                return 1;
            case INT32:
            case FLOAT:
                return 4;
            case TEXT:
            case INT64:
            case DOUBLE:
            case VECTOR:
                return 8;
            default:
                //TODO 抛出自定义异常
                // throw new UnSupportedDataTypeException(this.toString());
                return -1;
        }
    }

    public byte serialize() {
        return this.type;
    }

    public boolean isNumeric() {
        switch (this) {
            case BOOLEAN:
            case TEXT:
            case VECTOR:
                return false;
            case INT32:
            case FLOAT:
            case INT64:
            case DOUBLE:
                return true;
            default:
                //TODO 抛出自定义数据异常
                // throw new UnSupportedDataTypeException(this.toString());
                return false;
        }
    }
}
