package com.tsFuzzy.tsdbms.iotdb;

import com.fuzzy.GlobalState;
import com.fuzzy.Randomly;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.common.tsaf.util.GenerateTimestampUtil;
import com.fuzzy.iotdb.*;
import com.fuzzy.iotdb.ast.*;
import com.fuzzy.iotdb.gen.IotDBExpressionGenerator;
import com.fuzzy.iotdb.resultSet.IotDBResultSet;
import com.fuzzy.iotdb.util.IotDBValueStateConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

@Slf4j
public class TestIotDBUtil {

    public static String host = "111.229.183.22";
    public static int port = 6665;
    public static String userName = "root";
    public static String password = "root";

    @Test
    public void testIotDBSqlStatement() throws Exception {
        String sql = "CREATE TIMESERIES root.db1.t1 INT32 compressor=SNAPPY tags(8HOO=o2)";
        IotDBConnection IotDBConnection = new IotDBConnection(host, port, userName, password);
        try (IotDBStatement s = IotDBConnection.createStatement()) {
            IotDBResultSet iotDBResultSet = (IotDBResultSet) s.executeQuery(sql);
//            while (iotDBResultSet.getSessionDataSet().hasNext()) {
//                RowRecord next = iotDBResultSet.getSessionDataSet().next();
//                log.info("next:{}", next);
//            }
//            log.info("result:{}", iotDBResultSet.getSessionDataSet());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testConnect() throws Exception {
        Session session = new Session.Builder()
                .host("172.29.185.200")
                .port(6665).build();
        session.open();
        session.executeQueryStatement("SHOW DATABASES");
    }

    @Test
    public void testExecuteQuery() throws Exception {
        String sql = "SELECT MAX_VALUE(t1) AS ref0 FROM root.db0 GROUP BY ([1641024000000, 1641024015000),20s,1s);";
        IotDBConnection IotDBConnection = new IotDBConnection(host, port, userName, password);
        try (IotDBStatement s = IotDBConnection.createStatement()) {
            IotDBResultSet iotDBResultSet = (IotDBResultSet) s.executeQuery(sql);
            while (iotDBResultSet.getSessionDataSet().hasNext()) {
                RowRecord next = iotDBResultSet.getSessionDataSet().next();
                log.info("next:{}", next);
            }
            log.info("result:{}", iotDBResultSet.getSessionDataSet());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRandomlyGetIntegerRange() {
        GlobalState globalState = new IotDBGlobalState();
        globalState.setRandomly(new Randomly(1));
        for (int i = 0; i < 50; i++) {
            log.warn("{}", globalState.getRandomly().getInteger(1, 4));
        }
    }

    @Test
    public void testExpression() {
        IotDBBinaryComparisonOperation iotDBBinaryComparisonOperation = new IotDBBinaryComparisonOperation(
                IotDBConstant.createDoubleConstant(0.8043953),
                IotDBConstant.createDoubleConstant(0.8043954),
                IotDBBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);

        IotDBConstant expectedValue = iotDBBinaryComparisonOperation.getExpectedValue();

        log.info("res:{}", true ^ false);
    }

    @Test
    public void testFloat() {
        float a = 0.5022F;
        double b = (double) a;
        log.info("{}", new BigDecimal(b).compareTo(new BigDecimal(a)));
    }

    @Test
    public void timeGenerate() {
        long startTimestamp = 1641023921857L;
        long endTimestamp = 1641024350154L;
        long samplingFrequency = 379000;
        List<Long> timestamps = GenerateTimestampUtil.genTimestampsInRange(startTimestamp, endTimestamp, samplingFrequency);
        log.info("{}", timestamps.size());
    }

    @Test
    public void testColumnConstraint() throws Exception {
        // 32860 < time - time
        IotDBColumnReference timeReference = new IotDBColumnReference(
                new IotDBSchema.IotDBColumn(IotDBValueStateConstant.TIME_FIELD.getValue(),
                        false, IotDBSchema.IotDBDataType.INT64), IotDBConstant.createInt64Constant(1));
        IotDBColumnReference columnReference = new IotDBColumnReference(
                new IotDBSchema.IotDBColumn("c0",
                        false, IotDBSchema.IotDBDataType.INT32), IotDBConstant.createInt32Constant(1));

        IotDBConstant constant1 = IotDBConstant.createIntConstant(86997L);
        IotDBConstant constant2 = IotDBConstant.createIntConstant(5708L);
        IotDBConstant constant3 = IotDBConstant.createIntConstant(-32860L);

        IotDBBinaryArithmeticOperation binaryArithmeticOperation1 = new IotDBBinaryArithmeticOperation(timeReference,
                timeReference, IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator.SUBTRACT);
        IotDBBinaryArithmeticOperation binaryArithmeticOperation2 = new IotDBBinaryArithmeticOperation(binaryArithmeticOperation1,
                constant2, IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator.SUBTRACT);
        IotDBBinaryArithmeticOperation binaryArithmeticOperation3 = new IotDBBinaryArithmeticOperation(binaryArithmeticOperation2,
                constant3, IotDBBinaryArithmeticOperation.IotDBBinaryArithmeticOperator.DIVIDE);
        IotDBUnaryNotPrefixOperation notPrefixOperator = new IotDBUnaryNotPrefixOperation(binaryArithmeticOperation3,
                IotDBUnaryNotPrefixOperation.IotDBUnaryNotPrefixOperator.NOT_INT);
        IotDBBinaryComparisonOperation comparisonOperation = new IotDBBinaryComparisonOperation(constant3,
                binaryArithmeticOperation1, IotDBBinaryComparisonOperation.BinaryComparisonOperator.LESS);
        IotDBInOperation inOperation = new IotDBInOperation(constant1, Arrays.asList(constant2), false);

        TimeSeriesConstraint res = IotDBVisitor.asConstraint("", "",
                comparisonOperation, new HashSet<>());
        String str = IotDBVisitor.asString(comparisonOperation);
        log.info("ColumnConstraint:{} \n str:{}", res, str);
    }

    @Test
    public void testSyntaxValidity() throws Exception {
        IotDBGlobalState state = new IotDBGlobalState();
        state.setDbmsSpecificOptions(new IotDBOptions());
        state.setRandomly(new Randomly());
        IotDBSchema.IotDBColumn column = new IotDBSchema.IotDBColumn("t1",
                false, IotDBSchema.IotDBDataType.INT64);
        IotDBExpressionGenerator generator = new IotDBExpressionGenerator(state).setColumns(Arrays.asList(column));

        IotDBConnection iotDBConnection = new IotDBConnection(host, port, userName, password);
        String sql = "SELECT * FROM root.db0 WHERE ";
        executeQuerySuccess(iotDBConnection, sql + "t1 >= 0");

        for (String combinationExpressionKey : IotDBExpressionGenerator.pairingProhibited) {
            log.info("combinationExpressionKey: {}", combinationExpressionKey);
            String[] combinationExpressions = combinationExpressionKey.split("__");
            if (!combinationExpressions[0].equalsIgnoreCase("UNARY_PREFIX_OPERATION")) continue;

            for (int i = 0; i < 200; i++) {
                try {
                    IotDBExpression expression = generator.generateExpressionForSyntaxValidity(
                            combinationExpressions[0], combinationExpressions[1]);
                    if (i == 0) log.info("expression: {}", sql + IotDBVisitor.asString(expression));

                    IotDBExpression isNullExpression = expression;
                    IotDBExpression trueExpression = expression;
                    IotDBExpression falseExpression = expression;

                    try {
                        isNullExpression = new IotDBUnaryPostfixOperation(expression,
                                IotDBUnaryPostfixOperation.UnaryPostfixOperator.IS_NULL, false);
                        trueExpression = expression;
                        falseExpression = IotDBUnaryNotPrefixOperation.getNotUnaryPrefixOperation(expression);
                    } catch (Exception e) {

                    }

                    String isNullExpressionString = IotDBVisitor.asString(isNullExpression);
                    String trueExpressionString = IotDBVisitor.asString(trueExpression);
                    String falseExpressionString = IotDBVisitor.asString(falseExpression);
                    if (executeQuerySuccess(iotDBConnection, sql + isNullExpressionString)
                            || executeQuerySuccess(iotDBConnection, sql + trueExpressionString)
                            || executeQuerySuccess(iotDBConnection, sql + falseExpressionString)) {
                        log.error("expression execute success: \n isNullExpressionString: {} " +
                                        "\n trueExpressionString: {} \n falseExpressionString: {}",
                                sql + isNullExpressionString,
                                sql + trueExpressionString, sql + falseExpressionString);
                        i = Integer.MAX_VALUE - 1;
                    }
                } catch (Exception e) {
                    log.warn("e:", e);
                }
            }
        }

    }

    private boolean executeQuerySuccess(IotDBConnection iotDBConnection, String sql) throws Exception {
        try (IotDBStatement s = iotDBConnection.createStatement()) {
            IotDBResultSet iotDBResultSet = (IotDBResultSet) s.executeQuery(sql);
            while (iotDBResultSet.hasNext()) {
                RowRecord currentValue = iotDBResultSet.getCurrentValue();
                return true;
            }
            return false;
        } catch (SQLException e) {
            return false;
        }
    }
}
