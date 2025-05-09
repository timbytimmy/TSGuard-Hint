package com.fuzzy.influxdb.util;

import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBDataType;
import com.fuzzy.influxdb.ast.InfluxDBColumnReference;
import com.fuzzy.influxdb.ast.InfluxDBConstant.InfluxDBTextConstant;
import com.fuzzy.influxdb.ast.InfluxDBExpression;
import com.fuzzy.influxdb.constant.BinaryNodeIsString;

public class InfluxDBBinaryOperationUtil {

    /**
     * @param left
     * @param right
     * @return int
     * @description // left is str: -1
     * // all is str: 0
     * // right is str: 1
     * // no str: 2
     * @dateTime 2024/4/4 20:25
     */
    public static BinaryNodeIsString checkNodeIsString(InfluxDBExpression left, InfluxDBExpression right) {

        if ((left instanceof InfluxDBTextConstant || nodeInstanceofColumnWithString(left))
                && (right instanceof InfluxDBTextConstant || nodeInstanceofColumnWithString(right)))
            return BinaryNodeIsString.ALL;
        else if (left instanceof InfluxDBTextConstant || nodeInstanceofColumnWithString(left))
            return BinaryNodeIsString.LEFT;
        else if (right instanceof InfluxDBTextConstant || nodeInstanceofColumnWithString(right))
            return BinaryNodeIsString.RIGHT;
        else return BinaryNodeIsString.NONE;
    }

    private static boolean nodeInstanceofColumnWithString(InfluxDBExpression node) {
        return node instanceof InfluxDBColumnReference &&
                InfluxDBDataType.STRING.equals(((InfluxDBColumnReference) node).getColumn().getType());
    }
}
