package com.tsFuzzy.tsdbms.tdengine;

import com.fuzzy.Randomly;
import com.fuzzy.TDengine.TDengineGlobalState;
import com.fuzzy.TDengine.TDengineOptions;
import com.fuzzy.TDengine.TDengineSchema;
import com.fuzzy.TDengine.TDengineVisitor;
import com.fuzzy.TDengine.ast.*;
import com.fuzzy.TDengine.gen.TDengineExpressionGenerator;
import com.fuzzy.TDengine.tsaf.enu.TDengineConstantString;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.tsaf.*;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequency;
import com.fuzzy.common.tsaf.samplingfrequency.SamplingFrequencyManager;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Slf4j
public class TestTDengineUtil {

    public static String host = "172.29.185.200";
    public static int port = 6041;
    public static String database = "tsafdb0";
    public static String userName = "root";
    public static String password = "taosdata";
    public static String jdbcUrl;

    {
        jdbcUrl = String.format("jdbc:TAOS-RS://%s:%d/%s?user=%s&password=%s", host, port, database, userName, password);
//        jdbcUrl = String.format("jdbc:TAOS-RS://%s:%d/?user=%s&password=%s", host, port, userName, password);
    }

    // write other module
    @Test
    public void testTDengineSqlStatement() throws Exception {
        // TDengine ERROR (0x334): sql: CREATE DATABASE IF NOT EXISTS pqsdbconnectiontest, desc: Out of dnodes
        String sql = "CREATE STABLE IF NOT EXISTS super_t1( time TIMESTAMP, deviceId INT, c0 BIGINT ) TAGS (location BINARY(64), group_id INT);CREATE TABLE t1 USING super_t1 TAGS ('', 1693637208);";
        Connection conn = getTDengineConnection();
        try (Statement statement = conn.createStatement()) {
            // executeQuery
//            ResultSet resultSet = statement.executeQuery(sql);
//            while (resultSet.next()) {
//                log.info("result:{}", resultSet.getString("name"));
//            }
            // create database
            statement.execute(sql);
//            log.info(JSONObject.toJSONString(resultSet));
        } catch (Exception e) {
            log.error("sql execute error:", e);
        }
    }

    @Test
    public void dropDatabaseTest() throws Exception {
        jdbcUrl = String.format("jdbc:TAOS-RS://%s:%d/?user=%s&password=%s", host, port, userName, password);
        String sql = "show databases;";
        Connection conn = getTDengineConnection();
        try (Statement statement = conn.createStatement()) {
            // executeQuery
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                String databaseName = resultSet.getString("name");
                if (databaseName.startsWith("tsafdb") || databaseName.startsWith("pqsdb"))
                    statement.execute(String.format("drop database %s;", databaseName));
                log.info("result:{}", databaseName);
            }
            // create database
//            statement.execute(sql);
//            log.info(JSONObject.toJSONString(resultSet));
        } catch (Exception e) {
            log.error("sql execute error:", e);
        }
    }

    @Test
    public void testTDengineConnect() throws Exception {
        // curl -L -u root:taosdata -d "select name, ntables, status from information_schema.ins_databases;" localhost:6041/rest/sql
        Connection conn = getTDengineConnection();
        System.out.println("Connected");
        conn.close();
    }

    private Connection getTDengineConnection() throws Exception {
        return DriverManager.getConnection(jdbcUrl);
    }

    @Test
    public void test() {
        int count = 0;
        for (int i = 0; i < 1000; i++) {
            if (Randomly.getBoolean(9, 10)) count++;
        }
        log.info("{}", count);
    }

    @Test
    public void testFileWriter() throws Exception {
        File file = new File("E:\\work project\\t3-tsms\\tsFuzzy\\logs\\TDengine\\pqsdb.log");
        FileWriter fileWriter = new FileWriter(file);
        fileWriter.write("-- pivot row values:;\n" +
                "-- t1\\r--\tt1.c0: UINT=NULL\\r--\tt1.c2: UINT=2539444514\\r--\tt1.time: TIMESTAMP=1641024021000\\r--\tt1.c1: VARCHAR=NULL;\n" +
                "--\\r\\r-- rectified predicates and their expected values:\\r\\r--(NOT (\t\t(((+ (668780393029884006)) = ((+ (668780393029884006))))) AND (((NOT((318513548) < (c2)))) OR ((c0) IS NULL)) -- FALSE\\r-- ( =  ((+ (\t\t\t\t\t668780393029884006 -- 668780393029884006\\r-- ))))\t\t\t((NOT((318513548) < (c2)))) OR ((c0) IS NULL) -- TRUE\\r-- (NOT (\t\t\t\t\t(318513548) < (c2) -- FALSE\\r-- \t\t\t\t\t\t318513548 -- 318513548\\r-- \t\t\t\t\t\tc2 -- 2539444514\\r-- ))\t\t\t\t(c0) IS NULL -- TRUE\\r-- \t\t\t\t\tc0 -- NULL\\r-- ))\\r;\n" +
                "DROP DATABASE IF EXISTS pqsdb1;\n");
        fileWriter.flush();
    }

    @Test
    public void testColumnConstraint() throws Exception {
        TDengineColumnReference timeReference = new TDengineColumnReference(
                new TDengineSchema.TDengineColumn(TDengineConstantString.TIME_FIELD_NAME.getName(),
                        false, TDengineSchema.TDengineDataType.BIGINT), TDengineConstant.createUInt32Constant(1));
        TDengineColumnReference columnReference = new TDengineColumnReference(
                new TDengineSchema.TDengineColumn("c0",
                        false, TDengineSchema.TDengineDataType.BIGINT), TDengineConstant.createUInt32Constant(1));

        TDengineConstant left = TDengineConstant.createUInt32Constant(1641024014000L);
        TDengineBinaryComparisonOperation comparisonOperation = new TDengineBinaryComparisonOperation(left, timeReference,
                TDengineBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        TDengineConstant right = TDengineConstant.createUInt32Constant(1641024077000L);
        TDengineBinaryComparisonOperation comparisonOperation2 = new TDengineBinaryComparisonOperation(timeReference, right,
                TDengineBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        TDengineBinaryLogicalOperation logicalAndOperation = new TDengineBinaryLogicalOperation(comparisonOperation,
                comparisonOperation2, TDengineBinaryLogicalOperation.TDengineBinaryLogicalOperator.AND);

        TDengineConstant left1 = TDengineConstant.createUInt32Constant(1);
        TDengineBinaryComparisonOperation comparisonOperation3 = new TDengineBinaryComparisonOperation(left1, columnReference,
                TDengineBinaryComparisonOperation.BinaryComparisonOperator.LESS);
        TDengineConstant right1 = TDengineConstant.createUInt32Constant(1690394744832L);
        TDengineBinaryComparisonOperation comparisonOperation4 = new TDengineBinaryComparisonOperation(columnReference, right1,
                TDengineBinaryComparisonOperation.BinaryComparisonOperator.LESS);
        TDengineBinaryLogicalOperation logicalAndOperation2 = new TDengineBinaryLogicalOperation(comparisonOperation3,
                comparisonOperation4, TDengineBinaryLogicalOperation.TDengineBinaryLogicalOperator.AND);

        TDengineBinaryLogicalOperation logicalOrOperation = new TDengineBinaryLogicalOperation(logicalAndOperation,
                logicalAndOperation2, TDengineBinaryLogicalOperation.TDengineBinaryLogicalOperator.OR);

        // constraint 1
        TimeSeriesConstraint constraint = TDengineVisitor.asConstraint("", "",
                logicalOrOperation, null);

        // constraint for cast operation
        TDengineCastOperation castOperation = new TDengineCastOperation(columnReference,
                TDengineCastOperation.CastType.VARCHAR);
        TDengineCastOperation castOperation2 = new TDengineCastOperation(columnReference,
                TDengineCastOperation.CastType.VARCHAR);
        TDengineBinaryComparisonOperation comparisonOperationForCast = new TDengineBinaryComparisonOperation(
                castOperation, castOperation2, TDengineBinaryComparisonOperation.BinaryComparisonOperator.EQUALS);
        // (( CAST(c0 as VARCHAR) = ( CAST(c0 as VARCHAR)))) AND ((time) NOT IN (1671167829075))
        TDengineInOperation inOperationForCast = new TDengineInOperation(timeReference, Arrays.asList(left),
                false);
        TDengineBinaryLogicalOperation and = new TDengineBinaryLogicalOperation(comparisonOperationForCast,
                inOperationForCast, TDengineBinaryLogicalOperation.TDengineBinaryLogicalOperator.AND);
        TimeSeriesConstraint constraintForCast = TDengineVisitor.asConstraint("", "",
                and, null);

        // constraint for arithmetic operator
        // (9 * c1) + 9 <= 1641024077000L
        TDengineBinaryArithmeticOperation arithmeticOperation = new TDengineBinaryArithmeticOperation(columnReference,
                right1, TDengineBinaryArithmeticOperation.TDengineBinaryArithmeticOperator.MULTIPLY);
        TDengineBinaryArithmeticOperation arithmeticOperation2 = new TDengineBinaryArithmeticOperation(timeReference,
                right1, TDengineBinaryArithmeticOperation.TDengineBinaryArithmeticOperator.PLUS);
        TDengineBinaryComparisonOperation comparisonOperation5 = new TDengineBinaryComparisonOperation(arithmeticOperation2, right,
                TDengineBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);

        TimeSeriesConstraint constraintForArithmetic = TDengineVisitor.asConstraint("", "",
                comparisonOperation5, null);

        // unary prefix + -
        TDengineUnaryPrefixOperation unaryPrefixOperation = new TDengineUnaryPrefixOperation(columnReference,
                TDengineUnaryPrefixOperation.TDengineUnaryPrefixOperator.MINUS);
        TDengineBinaryComparisonOperation comparisonOperation6 = new TDengineBinaryComparisonOperation(unaryPrefixOperation, right,
                TDengineBinaryComparisonOperation.BinaryComparisonOperator.LESS_EQUALS);
        TimeSeriesConstraint constraintForUnaryPrefix = TDengineVisitor.asConstraint("", "",
                comparisonOperation6, null);

        // between
        TDengineBinaryArithmeticOperation arithmeticOperation3 = new TDengineBinaryArithmeticOperation(left1,
                right1, TDengineBinaryArithmeticOperation.TDengineBinaryArithmeticOperator.PLUS);
        TDengineBetweenOperation betweenOperation = new TDengineBetweenOperation(timeReference, arithmeticOperation3, right1,
                false);
        TimeSeriesConstraint constraintForBetween = TDengineVisitor.asConstraint("", "",
                betweenOperation, null);

        // in
        TDengineInOperation inOperation = new TDengineInOperation(columnReference, Arrays.asList(left1, right1, left),
                true);
        TimeSeriesConstraint constraintForIn = TDengineVisitor.asConstraint("", "",
                inOperation, null);

        // NOT
        TDengineUnaryNotPrefixOperation notPrefixOperation = new TDengineUnaryNotPrefixOperation(columnReference,
                TDengineUnaryNotPrefixOperation.TDengineUnaryNotPrefixOperator.NOT_INT);
        log.info("{}", constraint);
    }

    @Test
    public void testConstraint() {
        // (((682852280) / (c0)) NOT IN (-0.5479925, 2.682375))
        // {"columnName":"TRUE","empty":true,"notEqualValues":[],"rangeConstraints":[]}
        TDengineColumnReference timeReference = new TDengineColumnReference(
                new TDengineSchema.TDengineColumn(TDengineConstantString.TIME_FIELD_NAME.getName(),
                        false, TDengineSchema.TDengineDataType.BIGINT), TDengineConstant.createUInt32Constant(1));
        TDengineColumnReference columnReference = new TDengineColumnReference(
                new TDengineSchema.TDengineColumn(GlobalConstant.BASE_TIME_SERIES_NAME,
                        false, TDengineSchema.TDengineDataType.BIGINT), TDengineConstant.createUInt32Constant(0));
//        Set<Long> nullValues = new HashSet<>(Arrays.asList(1641024000000L, 1641024010000L, 1641024020000L,
//                1641024030000L, 1641024040000L));
        Set<Long> nullValues = new HashSet<>(Arrays.asList(1L, 3L, 5L, 7L, 9L));

        TDengineConstant constant1 = TDengineConstant.createInt32Constant(-1L);
        TDengineConstant constant2 = TDengineConstant.createUInt32Constant(-3L);
        TDengineConstant constant3 = TDengineConstant.createUInt32Constant(1L);

        TDengineBinaryArithmeticOperation arithmeticOperation = new TDengineBinaryArithmeticOperation(constant1,
                columnReference, TDengineBinaryArithmeticOperation.TDengineBinaryArithmeticOperator.DIVIDE);
        TDengineBetweenOperation betweenOperation = new TDengineBetweenOperation(arithmeticOperation, constant2,
                constant3, false);

        TimeSeriesConstraint res = TDengineVisitor.asConstraint("", "", betweenOperation, nullValues);
        log.info("{}", res);
    }

    @Test
    public void testSyntaxValidity() throws Exception {
        TDengineGlobalState state = new TDengineGlobalState();
        state.setDbmsSpecificOptions(new TDengineOptions());
        state.setRandomly(new Randomly());
        TDengineSchema.TDengineColumn column = new TDengineSchema.TDengineColumn("c1",
                false, TDengineSchema.TDengineDataType.BIGINT);
        TDengineExpressionGenerator generator = new TDengineExpressionGenerator(state).setColumns(Arrays.asList(column));

        // connection
        Connection conn = getTDengineConnection();
        String sql = "select * from t1 where TRUE";
        executeQuerySuccess(conn, sql);

        sql = "select * from t1 where ";
        for (String combinationExpressionKey : TDengineExpressionGenerator.pairingProhibited) {
            log.info("combinationExpressionKey: {}", combinationExpressionKey);
            String[] combinationExpressions = combinationExpressionKey.split("__");
            if (!(combinationExpressions[0].equalsIgnoreCase("BINARY_ARITHMETIC_OPERATION")
                    && combinationExpressions[1].equalsIgnoreCase("UNARY_POSTFIX")))
                continue;

            for (int i = 0; i < 5000; i++) {
                TDengineExpression expression = generator.generateExpressionForSyntaxValidity(
                        combinationExpressions[0], combinationExpressions[1]);
                String predicationExpression = TDengineVisitor.asString(expression);
                if (executeQuerySuccess(conn, sql + predicationExpression)) {
                    log.error("expression execute success: {}", sql + predicationExpression);
                }
                if (i == 1) log.info("expression: {}", sql + predicationExpression);
            }
        }

    }

    private boolean executeQuerySuccess(Connection conn, String sql) {
        try (Statement statement = conn.createStatement()) {
            // executeQuery
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                log.error("result:{}", resultSet.getString("REF0"));
            }
            return true;
        } catch (Exception e) {
            if (!e.getMessage().contains("syntax error near")) log.warn("sql execute error:", e);
            return false;
        }
    }

    @Test
    public void testQuery() throws Exception {
        Connection conn = getTDengineConnection();
        String sql = "SELECT c0 AS REF0, time FROM t1 WHERE (c0 < 1)";
        executeQuerySuccess(conn, sql);
    }

    @Test
    public void testMod() {
        long a = Long.MAX_VALUE;
        double b = 0.08638607;
        log.info("{}", a % b);
    }

    @Test
    public void testFloat() {
        long a = 2424029746057421827L;
//        8991644589450262000
//        8991644589450261000
        long b = 766969125L;

        log.info("{}", "15.0".compareTo("15"));
        log.info("{} {} {}", a, b, a % b);
    }

    @Test
    public void testColumnConstraintComplement() {
        TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint("test");
        timeSeriesConstraint.setRangeConstraints(Arrays.asList(
                new RangeConstraint(new BigDecimal(Long.MIN_VALUE), new BigDecimal(9)),
                new RangeConstraint(new BigDecimal(11), new BigDecimal(20))));
        List<BigDecimal> notEqualValues = new ArrayList<>();
        notEqualValues.add(new BigDecimal(25));
        notEqualValues.add(new BigDecimal(25));
        timeSeriesConstraint.setNotEqualValues(notEqualValues);
        TimeSeriesConstraint complement = timeSeriesConstraint.complement();
        log.info("{}", complement);
    }

    @Test
    public void genTestData() throws Exception {
        long startTimestamp = 1640966400000L;
        long endTimestamp = 1641028995000L;
        long samplingFrequency = 5000L;
        long value = 1;

        executeSQL("DROP TABLE IF EXISTS t2;");
        executeSQL("CREATE TABLE t2(time TIMESTAMP, c1 BIGINT);");

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO t2(time, c1) VALUES");
        for (long i = startTimestamp; i <= endTimestamp; i += samplingFrequency) {
            sb.append("(").append(i).append(",").append(value++).append("), ");
        }
        log.info("{}", sb.substring(0, sb.length() - 2));
        executeSQL(sb.substring(0, sb.length() - 2));
    }

    private void executeSQL(String sql) throws Exception {
        Connection conn = getTDengineConnection();
        try (Statement statement = conn.createStatement()) {
            // create database
            statement.execute(sql);
        } catch (Exception e) {
            log.error("sql execute error:", e);
        }
    }

    @Test
    public void confirmStartTimestamp() throws ParseException {
        String date = "2021-12-31 23:20:00.000";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        long timestamp = sdf.parse(date).getTime();
        log.info("{}", timestamp);

        // 1640966292000
        // 1640908800000
        // 57492000
        long[] startTimestamps = {1640880000000L, 1640883600000L, 1640887200000L, 1640890800000L, 1640894400000L,
                1640898000000L, 1640901600000L, 1640905200000L, 1640908800000L};

        long interval = 3000;
        for (int i = 0; i < startTimestamps.length; i++) {
            if ((timestamp - startTimestamps[i]) % interval == 0) log.info("TRUE");
            else log.info("mod: {}", (timestamp - startTimestamps[i]) % interval);
        }
    }

    @Test
    public void testTDengineInterval() throws Exception {
        Connection conn = getTDengineConnection();
        long second = 0;
        for (int i = 0; i < 100; i++) {
            second += 10;
            String sql = String.format("select count(*), _wstart, _wend, _wduration from t2 WHERE time >= 1640966400000" +
                    " AND time <= 1640970000000 INTERVAL(%ds);", second);

            try (Statement statement = conn.createStatement()) {
                ResultSet resultSet = statement.executeQuery(sql);
                while (resultSet.next()) {
                    String date = resultSet.getString(2);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    long timestamp = sdf.parse(date).getTime();
                    if ((timestamp - 1640908800000L) % second != 0) log.error("FALSE, second:{}", second);
                    break;
                }
            } catch (Exception e) {
                if (!e.getMessage().contains("syntax error near")) log.warn("sql execute error:", e);
            }
        }
    }

    @Test
    public void genData() throws Exception {
        executeSQL("DROP TABLE IF EXISTS t1;");
        executeSQL("CREATE TABLE IF NOT EXISTS t1( time TIMESTAMP, deviceId INT, c0 INT , c1 INT , c2 INT ) TTL 0 COMMENT 'jgN6sU' ;");
        String sql = "INSERT INTO t1(time, deviceid, c0, c1, c2) VALUES(1641024000000, 1, 0, 0, 0), (1641024005000, 1, 1, 1, 1), (1641024010000, 1, 2, 2, 2), (1641024015000, 1, 3, 3, 3), (1641024020000, 1, 4, 4, 4), (1641024025000, 1, 5, 5, 5), (1641024030000, 1, 6, 6, 6), (1641024035000, 1, 7, 7, 7), (1641024040000, 1, 8, 8, 8), (1641024045000, 1, 9, 9, 9), (1641024050000, 1, 10, 10, 10), (1641024055000, 1, 11, 11, 11), (1641024060000, 1, 12, 12, 12), (1641024065000, 1, 13, 13, 13), (1641024070000, 1, 14, 14, 14), (1641024075000, 1, 15, 15, 15), (1641024080000, 1, 16, 16, 16), (1641024085000, 1, 17, 17, 17), (1641024090000, 1, 18, 18, 18), (1641024095000, 1, 19, 19, 19), (1641024100000, 1, 20, 20, 20), (1641024105000, 1, 21, 21, 21), (1641024110000, 1, 22, 22, 22), (1641024115000, 1, 23, 23, 23), (1641024120000, 1, 24, 24, 24), (1641024125000, 1, 25, 25, 25), (1641024130000, 1, 26, 26, 26), (1641024135000, 1, 27, 27, 27), (1641024140000, 1, 28, 28, 28), (1641024145000, 1, 29, 29, 29), (1641024150000, 1, 30, 30, 30), (1641024155000, 1, 31, 31, 31), (1641024160000, 1, 32, 32, 32), (1641024165000, 1, 33, 33, 33), (1641024170000, 1, 34, 34, 34), (1641024175000, 1, 35, 35, 35), (1641024180000, 1, 36, 36, 36), (1641024185000, 1, 37, 37, 37), (1641024190000, 1, 38, 38, 38), (1641024195000, 1, 39, 39, 39), (1641024200000, 1, 40, 40, 40), (1641024205000, 1, 41, 41, 41), (1641024210000, 1, 42, 42, 42), (1641024215000, 1, 43, 43, 43), (1641024220000, 1, 44, 44, 44), (1641024225000, 1, 45, 45, 45), (1641024230000, 1, 46, 46, 46), (1641024235000, 1, 47, 47, 47), (1641024240000, 1, 48, 48, 48), (1641024245000, 1, 49, 49, 49), (1641024250000, 1, 50, 50, 50), (1641024255000, 1, 51, 51, 51), (1641024260000, 1, 52, 52, 52), (1641024265000, 1, 53, 53, 53), (1641024270000, 1, 54, 54, 54), (1641024275000, 1, 55, 55, 55), (1641024280000, 1, 56, 56, 56), (1641024285000, 1, 57, 57, 57), (1641024290000, 1, 58, 58, 58), (1641024295000, 1, 59, 59, 59), (1641024300000, 1, 60, 60, 60), (1641024305000, 1, 61, 61, 61), (1641024310000, 1, 62, 62, 62), (1641024315000, 1, 63, 63, 63), (1641024320000, 1, 64, 64, 64), (1641024325000, 1, 65, 65, 65), (1641024330000, 1, 66, 66, 66), (1641024335000, 1, 67, 67, 67), (1641024340000, 1, 68, 68, 68), (1641024345000, 1, 69, 69, 69), (1641024350000, 1, 70, 70, 70), (1641024355000, 1, 71, 71, 71), (1641024360000, 1, 72, 72, 72), (1641024365000, 1, 73, 73, 73), (1641024370000, 1, 74, 74, 74), (1641024375000, 1, 75, 75, 75), (1641024380000, 1, 76, 76, 76), (1641024385000, 1, 77, 77, 77), (1641024390000, 1, 78, 78, 78), (1641024395000, 1, 79, 79, 79), (1641024400000, 1, 80, 80, 80), (1641024405000, 1, 81, 81, 81), (1641024410000, 1, 82, 82, 82), (1641024415000, 1, 83, 83, 83), (1641024420000, 1, 84, 84, 84), (1641024425000, 1, 85, 85, 85), (1641024430000, 1, 86, 86, 86), (1641024435000, 1, 87, 87, 87), (1641024440000, 1, 88, 88, 88), (1641024445000, 1, 89, 89, 89), (1641024450000, 1, 90, 90, 90), (1641024455000, 1, 91, 91, 91), (1641024460000, 1, 92, 92, 92), (1641024465000, 1, 93, 93, 93), (1641024470000, 1, 94, 94, 94), (1641024475000, 1, 95, 95, 95), (1641024480000, 1, 96, 96, 96), (1641024485000, 1, 97, 97, 97), (1641024490000, 1, 98, 98, 98), (1641024495000, 1, 99, 99, 99);\n" +
                "INSERT INTO t1(time, deviceid, c0, c1, c2) VALUES(1641024500000, 1, 100, 100, 100), (1641024505000, 1, 101, 101, 101), (1641024510000, 1, 102, 102, 102), (1641024515000, 1, 103, 103, 103), (1641024520000, 1, 104, 104, 104), (1641024525000, 1, 105, 105, 105), (1641024530000, 1, 106, 106, 106), (1641024535000, 1, 107, 107, 107), (1641024540000, 1, 108, 108, 108), (1641024545000, 1, 109, 109, 109), (1641024550000, 1, 110, 110, 110), (1641024555000, 1, 111, 111, 111), (1641024560000, 1, 112, 112, 112), (1641024565000, 1, 113, 113, 113), (1641024570000, 1, 114, 114, 114), (1641024575000, 1, 115, 115, 115), (1641024580000, 1, 116, 116, 116), (1641024585000, 1, 117, 117, 117), (1641024590000, 1, 118, 118, 118), (1641024595000, 1, 119, 119, 119), (1641024600000, 1, 120, 120, 120), (1641024605000, 1, 121, 121, 121), (1641024610000, 1, 122, 122, 122), (1641024615000, 1, 123, 123, 123), (1641024620000, 1, 124, 124, 124), (1641024625000, 1, 125, 125, 125), (1641024630000, 1, 126, 126, 126), (1641024635000, 1, 127, 127, 127), (1641024640000, 1, 128, 128, 128), (1641024645000, 1, 129, 129, 129), (1641024650000, 1, 130, 130, 130), (1641024655000, 1, 131, 131, 131), (1641024660000, 1, 132, 132, 132), (1641024665000, 1, 133, 133, 133), (1641024670000, 1, 134, 134, 134), (1641024675000, 1, 135, 135, 135), (1641024680000, 1, 136, 136, 136), (1641024685000, 1, 137, 137, 137), (1641024690000, 1, 138, 138, 138), (1641024695000, 1, 139, 139, 139), (1641024700000, 1, 140, 140, 140), (1641024705000, 1, 141, 141, 141), (1641024710000, 1, 142, 142, 142), (1641024715000, 1, 143, 143, 143), (1641024720000, 1, 144, 144, 144), (1641024725000, 1, 145, 145, 145), (1641024730000, 1, 146, 146, 146), (1641024735000, 1, 147, 147, 147), (1641024740000, 1, 148, 148, 148), (1641024745000, 1, 149, 149, 149), (1641024750000, 1, 150, 150, 150), (1641024755000, 1, 151, 151, 151), (1641024760000, 1, 152, 152, 152), (1641024765000, 1, 153, 153, 153), (1641024770000, 1, 154, 154, 154), (1641024775000, 1, 155, 155, 155), (1641024780000, 1, 156, 156, 156), (1641024785000, 1, 157, 157, 157), (1641024790000, 1, 158, 158, 158), (1641024795000, 1, 159, 159, 159), (1641024800000, 1, 160, 160, 160), (1641024805000, 1, 161, 161, 161), (1641024810000, 1, 162, 162, 162), (1641024815000, 1, 163, 163, 163), (1641024820000, 1, 164, 164, 164), (1641024825000, 1, 165, 165, 165), (1641024830000, 1, 166, 166, 166), (1641024835000, 1, 167, 167, 167), (1641024840000, 1, 168, 168, 168), (1641024845000, 1, 169, 169, 169), (1641024850000, 1, 170, 170, 170), (1641024855000, 1, 171, 171, 171), (1641024860000, 1, 172, 172, 172), (1641024865000, 1, 173, 173, 173), (1641024870000, 1, 174, 174, 174), (1641024875000, 1, 175, 175, 175), (1641024880000, 1, 176, 176, 176), (1641024885000, 1, 177, 177, 177), (1641024890000, 1, 178, 178, 178), (1641024895000, 1, 179, 179, 179), (1641024900000, 1, 180, 180, 180), (1641024905000, 1, 181, 181, 181), (1641024910000, 1, 182, 182, 182), (1641024915000, 1, 183, 183, 183), (1641024920000, 1, 184, 184, 184), (1641024925000, 1, 185, 185, 185), (1641024930000, 1, 186, 186, 186), (1641024935000, 1, 187, 187, 187), (1641024940000, 1, 188, 188, 188), (1641024945000, 1, 189, 189, 189), (1641024950000, 1, 190, 190, 190), (1641024955000, 1, 191, 191, 191), (1641024960000, 1, 192, 192, 192), (1641024965000, 1, 193, 193, 193), (1641024970000, 1, 194, 194, 194), (1641024975000, 1, 195, 195, 195), (1641024980000, 1, 196, 196, 196), (1641024985000, 1, 197, 197, 197), (1641024990000, 1, 198, 198, 198), (1641024995000, 1, 199, 199, 199);\n" +
                "INSERT INTO t1(time, deviceid, c0, c1, c2) VALUES(1641025000000, 1, 200, 200, 200), (1641025005000, 1, 201, 201, 201), (1641025010000, 1, 202, 202, 202), (1641025015000, 1, 203, 203, 203), (1641025020000, 1, 204, 204, 204), (1641025025000, 1, 205, 205, 205), (1641025030000, 1, 206, 206, 206), (1641025035000, 1, 207, 207, 207), (1641025040000, 1, 208, 208, 208), (1641025045000, 1, 209, 209, 209), (1641025050000, 1, 210, 210, 210), (1641025055000, 1, 211, 211, 211), (1641025060000, 1, 212, 212, 212), (1641025065000, 1, 213, 213, 213), (1641025070000, 1, 214, 214, 214), (1641025075000, 1, 215, 215, 215), (1641025080000, 1, 216, 216, 216), (1641025085000, 1, 217, 217, 217), (1641025090000, 1, 218, 218, 218), (1641025095000, 1, 219, 219, 219), (1641025100000, 1, 220, 220, 220), (1641025105000, 1, 221, 221, 221), (1641025110000, 1, 222, 222, 222), (1641025115000, 1, 223, 223, 223), (1641025120000, 1, 224, 224, 224), (1641025125000, 1, 225, 225, 225), (1641025130000, 1, 226, 226, 226), (1641025135000, 1, 227, 227, 227), (1641025140000, 1, 228, 228, 228), (1641025145000, 1, 229, 229, 229), (1641025150000, 1, 230, 230, 230), (1641025155000, 1, 231, 231, 231), (1641025160000, 1, 232, 232, 232), (1641025165000, 1, 233, 233, 233), (1641025170000, 1, 234, 234, 234), (1641025175000, 1, 235, 235, 235), (1641025180000, 1, 236, 236, 236), (1641025185000, 1, 237, 237, 237), (1641025190000, 1, 238, 238, 238), (1641025195000, 1, 239, 239, 239), (1641025200000, 1, 240, 240, 240), (1641025205000, 1, 241, 241, 241), (1641025210000, 1, 242, 242, 242), (1641025215000, 1, 243, 243, 243), (1641025220000, 1, 244, 244, 244), (1641025225000, 1, 245, 245, 245), (1641025230000, 1, 246, 246, 246), (1641025235000, 1, 247, 247, 247), (1641025240000, 1, 248, 248, 248), (1641025245000, 1, 249, 249, 249), (1641025250000, 1, 250, 250, 250), (1641025255000, 1, 251, 251, 251), (1641025260000, 1, 252, 252, 252), (1641025265000, 1, 253, 253, 253), (1641025270000, 1, 254, 254, 254), (1641025275000, 1, 255, 255, 255), (1641025280000, 1, 256, 256, 256), (1641025285000, 1, 257, 257, 257), (1641025290000, 1, 258, 258, 258), (1641025295000, 1, 259, 259, 259), (1641025300000, 1, 260, 260, 260), (1641025305000, 1, 261, 261, 261), (1641025310000, 1, 262, 262, 262), (1641025315000, 1, 263, 263, 263), (1641025320000, 1, 264, 264, 264), (1641025325000, 1, 265, 265, 265), (1641025330000, 1, 266, 266, 266), (1641025335000, 1, 267, 267, 267), (1641025340000, 1, 268, 268, 268), (1641025345000, 1, 269, 269, 269), (1641025350000, 1, 270, 270, 270), (1641025355000, 1, 271, 271, 271), (1641025360000, 1, 272, 272, 272), (1641025365000, 1, 273, 273, 273), (1641025370000, 1, 274, 274, 274), (1641025375000, 1, 275, 275, 275), (1641025380000, 1, 276, 276, 276), (1641025385000, 1, 277, 277, 277), (1641025390000, 1, 278, 278, 278), (1641025395000, 1, 279, 279, 279), (1641025400000, 1, 280, 280, 280), (1641025405000, 1, 281, 281, 281), (1641025410000, 1, 282, 282, 282), (1641025415000, 1, 283, 283, 283), (1641025420000, 1, 284, 284, 284), (1641025425000, 1, 285, 285, 285), (1641025430000, 1, 286, 286, 286), (1641025435000, 1, 287, 287, 287), (1641025440000, 1, 288, 288, 288), (1641025445000, 1, 289, 289, 289), (1641025450000, 1, 290, 290, 290), (1641025455000, 1, 291, 291, 291), (1641025460000, 1, 292, 292, 292), (1641025465000, 1, 293, 293, 293), (1641025470000, 1, 294, 294, 294), (1641025475000, 1, 295, 295, 295), (1641025480000, 1, 296, 296, 296), (1641025485000, 1, 297, 297, 297), (1641025490000, 1, 298, 298, 298), (1641025495000, 1, 299, 299, 299);\n" +
                "INSERT INTO t1(time, deviceid, c0, c1, c2) VALUES(1641025500000, 1, 300, 300, 300), (1641025505000, 1, 301, 301, 301), (1641025510000, 1, 302, 302, 302), (1641025515000, 1, 303, 303, 303), (1641025520000, 1, 304, 304, 304), (1641025525000, 1, 305, 305, 305), (1641025530000, 1, 306, 306, 306), (1641025535000, 1, 307, 307, 307), (1641025540000, 1, 308, 308, 308), (1641025545000, 1, 309, 309, 309), (1641025550000, 1, 310, 310, 310), (1641025555000, 1, 311, 311, 311), (1641025560000, 1, 312, 312, 312), (1641025565000, 1, 313, 313, 313), (1641025570000, 1, 314, 314, 314), (1641025575000, 1, 315, 315, 315), (1641025580000, 1, 316, 316, 316), (1641025585000, 1, 317, 317, 317), (1641025590000, 1, 318, 318, 318), (1641025595000, 1, 319, 319, 319), (1641025600000, 1, 320, 320, 320), (1641025605000, 1, 321, 321, 321), (1641025610000, 1, 322, 322, 322), (1641025615000, 1, 323, 323, 323), (1641025620000, 1, 324, 324, 324), (1641025625000, 1, 325, 325, 325), (1641025630000, 1, 326, 326, 326), (1641025635000, 1, 327, 327, 327), (1641025640000, 1, 328, 328, 328), (1641025645000, 1, 329, 329, 329), (1641025650000, 1, 330, 330, 330), (1641025655000, 1, 331, 331, 331), (1641025660000, 1, 332, 332, 332), (1641025665000, 1, 333, 333, 333), (1641025670000, 1, 334, 334, 334), (1641025675000, 1, 335, 335, 335), (1641025680000, 1, 336, 336, 336), (1641025685000, 1, 337, 337, 337), (1641025690000, 1, 338, 338, 338), (1641025695000, 1, 339, 339, 339), (1641025700000, 1, 340, 340, 340), (1641025705000, 1, 341, 341, 341), (1641025710000, 1, 342, 342, 342), (1641025715000, 1, 343, 343, 343), (1641025720000, 1, 344, 344, 344), (1641025725000, 1, 345, 345, 345), (1641025730000, 1, 346, 346, 346), (1641025735000, 1, 347, 347, 347), (1641025740000, 1, 348, 348, 348), (1641025745000, 1, 349, 349, 349), (1641025750000, 1, 350, 350, 350), (1641025755000, 1, 351, 351, 351), (1641025760000, 1, 352, 352, 352), (1641025765000, 1, 353, 353, 353), (1641025770000, 1, 354, 354, 354), (1641025775000, 1, 355, 355, 355), (1641025780000, 1, 356, 356, 356), (1641025785000, 1, 357, 357, 357), (1641025790000, 1, 358, 358, 358), (1641025795000, 1, 359, 359, 359), (1641025800000, 1, 360, 360, 360), (1641025805000, 1, 361, 361, 361), (1641025810000, 1, 362, 362, 362), (1641025815000, 1, 363, 363, 363), (1641025820000, 1, 364, 364, 364), (1641025825000, 1, 365, 365, 365), (1641025830000, 1, 366, 366, 366), (1641025835000, 1, 367, 367, 367), (1641025840000, 1, 368, 368, 368), (1641025845000, 1, 369, 369, 369), (1641025850000, 1, 370, 370, 370), (1641025855000, 1, 371, 371, 371), (1641025860000, 1, 372, 372, 372), (1641025865000, 1, 373, 373, 373), (1641025870000, 1, 374, 374, 374), (1641025875000, 1, 375, 375, 375), (1641025880000, 1, 376, 376, 376), (1641025885000, 1, 377, 377, 377), (1641025890000, 1, 378, 378, 378), (1641025895000, 1, 379, 379, 379), (1641025900000, 1, 380, 380, 380), (1641025905000, 1, 381, 381, 381), (1641025910000, 1, 382, 382, 382), (1641025915000, 1, 383, 383, 383), (1641025920000, 1, 384, 384, 384), (1641025925000, 1, 385, 385, 385), (1641025930000, 1, 386, 386, 386), (1641025935000, 1, 387, 387, 387), (1641025940000, 1, 388, 388, 388), (1641025945000, 1, 389, 389, 389), (1641025950000, 1, 390, 390, 390), (1641025955000, 1, 391, 391, 391), (1641025960000, 1, 392, 392, 392), (1641025965000, 1, 393, 393, 393), (1641025970000, 1, 394, 394, 394), (1641025975000, 1, 395, 395, 395), (1641025980000, 1, 396, 396, 396), (1641025985000, 1, 397, 397, 397), (1641025990000, 1, 398, 398, 398), (1641025995000, 1, 399, 399, 399);\n" +
                "INSERT INTO t1(time, deviceid, c0, c1, c2) VALUES(1641026000000, 1, 400, 400, 400), (1641026005000, 1, 401, 401, 401), (1641026010000, 1, 402, 402, 402), (1641026015000, 1, 403, 403, 403), (1641026020000, 1, 404, 404, 404), (1641026025000, 1, 405, 405, 405), (1641026030000, 1, 406, 406, 406), (1641026035000, 1, 407, 407, 407), (1641026040000, 1, 408, 408, 408), (1641026045000, 1, 409, 409, 409), (1641026050000, 1, 410, 410, 410), (1641026055000, 1, 411, 411, 411), (1641026060000, 1, 412, 412, 412), (1641026065000, 1, 413, 413, 413), (1641026070000, 1, 414, 414, 414), (1641026075000, 1, 415, 415, 415), (1641026080000, 1, 416, 416, 416), (1641026085000, 1, 417, 417, 417), (1641026090000, 1, 418, 418, 418), (1641026095000, 1, 419, 419, 419), (1641026100000, 1, 420, 420, 420), (1641026105000, 1, 421, 421, 421), (1641026110000, 1, 422, 422, 422), (1641026115000, 1, 423, 423, 423), (1641026120000, 1, 424, 424, 424), (1641026125000, 1, 425, 425, 425), (1641026130000, 1, 426, 426, 426), (1641026135000, 1, 427, 427, 427), (1641026140000, 1, 428, 428, 428), (1641026145000, 1, 429, 429, 429), (1641026150000, 1, 430, 430, 430), (1641026155000, 1, 431, 431, 431), (1641026160000, 1, 432, 432, 432), (1641026165000, 1, 433, 433, 433), (1641026170000, 1, 434, 434, 434), (1641026175000, 1, 435, 435, 435), (1641026180000, 1, 436, 436, 436), (1641026185000, 1, 437, 437, 437), (1641026190000, 1, 438, 438, 438), (1641026195000, 1, 439, 439, 439), (1641026200000, 1, 440, 440, 440), (1641026205000, 1, 441, 441, 441), (1641026210000, 1, 442, 442, 442), (1641026215000, 1, 443, 443, 443), (1641026220000, 1, 444, 444, 444), (1641026225000, 1, 445, 445, 445), (1641026230000, 1, 446, 446, 446), (1641026235000, 1, 447, 447, 447), (1641026240000, 1, 448, 448, 448), (1641026245000, 1, 449, 449, 449), (1641026250000, 1, 450, 450, 450), (1641026255000, 1, 451, 451, 451), (1641026260000, 1, 452, 452, 452), (1641026265000, 1, 453, 453, 453), (1641026270000, 1, 454, 454, 454), (1641026275000, 1, 455, 455, 455), (1641026280000, 1, 456, 456, 456), (1641026285000, 1, 457, 457, 457), (1641026290000, 1, 458, 458, 458), (1641026295000, 1, 459, 459, 459), (1641026300000, 1, 460, 460, 460), (1641026305000, 1, 461, 461, 461), (1641026310000, 1, 462, 462, 462), (1641026315000, 1, 463, 463, 463), (1641026320000, 1, 464, 464, 464), (1641026325000, 1, 465, 465, 465), (1641026330000, 1, 466, 466, 466), (1641026335000, 1, 467, 467, 467), (1641026340000, 1, 468, 468, 468), (1641026345000, 1, 469, 469, 469), (1641026350000, 1, 470, 470, 470), (1641026355000, 1, 471, 471, 471), (1641026360000, 1, 472, 472, 472), (1641026365000, 1, 473, 473, 473), (1641026370000, 1, 474, 474, 474), (1641026375000, 1, 475, 475, 475), (1641026380000, 1, 476, 476, 476), (1641026385000, 1, 477, 477, 477), (1641026390000, 1, 478, 478, 478), (1641026395000, 1, 479, 479, 479), (1641026400000, 1, 480, 480, 480), (1641026405000, 1, 481, 481, 481), (1641026410000, 1, 482, 482, 482), (1641026415000, 1, 483, 483, 483), (1641026420000, 1, 484, 484, 484), (1641026425000, 1, 485, 485, 485), (1641026430000, 1, 486, 486, 486), (1641026435000, 1, 487, 487, 487), (1641026440000, 1, 488, 488, 488), (1641026445000, 1, 489, 489, 489), (1641026450000, 1, 490, 490, 490), (1641026455000, 1, 491, 491, 491), (1641026460000, 1, 492, 492, 492), (1641026465000, 1, 493, 493, 493), (1641026470000, 1, 494, 494, 494), (1641026475000, 1, 495, 495, 495), (1641026480000, 1, 496, 496, 496), (1641026485000, 1, 497, 497, 497), (1641026490000, 1, 498, 498, 498), (1641026495000, 1, 499, 499, 499);\n" +
                "INSERT INTO t1(time, deviceid, c0, c1, c2) VALUES(1641026500000, 1, 500, 500, 500), (1641026505000, 1, 501, 501, 501), (1641026510000, 1, 502, 502, 502), (1641026515000, 1, 503, 503, 503), (1641026520000, 1, 504, 504, 504), (1641026525000, 1, 505, 505, 505), (1641026530000, 1, 506, 506, 506), (1641026535000, 1, 507, 507, 507), (1641026540000, 1, 508, 508, 508), (1641026545000, 1, 509, 509, 509), (1641026550000, 1, 510, 510, 510), (1641026555000, 1, 511, 511, 511), (1641026560000, 1, 512, 512, 512), (1641026565000, 1, 513, 513, 513), (1641026570000, 1, 514, 514, 514), (1641026575000, 1, 515, 515, 515), (1641026580000, 1, 516, 516, 516), (1641026585000, 1, 517, 517, 517), (1641026590000, 1, 518, 518, 518), (1641026595000, 1, 519, 519, 519), (1641026600000, 1, 520, 520, 520), (1641026605000, 1, 521, 521, 521), (1641026610000, 1, 522, 522, 522), (1641026615000, 1, 523, 523, 523), (1641026620000, 1, 524, 524, 524), (1641026625000, 1, 525, 525, 525), (1641026630000, 1, 526, 526, 526), (1641026635000, 1, 527, 527, 527), (1641026640000, 1, 528, 528, 528), (1641026645000, 1, 529, 529, 529), (1641026650000, 1, 530, 530, 530), (1641026655000, 1, 531, 531, 531), (1641026660000, 1, 532, 532, 532), (1641026665000, 1, 533, 533, 533), (1641026670000, 1, 534, 534, 534), (1641026675000, 1, 535, 535, 535), (1641026680000, 1, 536, 536, 536), (1641026685000, 1, 537, 537, 537), (1641026690000, 1, 538, 538, 538), (1641026695000, 1, 539, 539, 539), (1641026700000, 1, 540, 540, 540), (1641026705000, 1, 541, 541, 541), (1641026710000, 1, 542, 542, 542), (1641026715000, 1, 543, 543, 543), (1641026720000, 1, 544, 544, 544), (1641026725000, 1, 545, 545, 545), (1641026730000, 1, 546, 546, 546), (1641026735000, 1, 547, 547, 547), (1641026740000, 1, 548, 548, 548), (1641026745000, 1, 549, 549, 549), (1641026750000, 1, 550, 550, 550), (1641026755000, 1, 551, 551, 551), (1641026760000, 1, 552, 552, 552), (1641026765000, 1, 553, 553, 553), (1641026770000, 1, 554, 554, 554), (1641026775000, 1, 555, 555, 555), (1641026780000, 1, 556, 556, 556), (1641026785000, 1, 557, 557, 557), (1641026790000, 1, 558, 558, 558), (1641026795000, 1, 559, 559, 559), (1641026800000, 1, 560, 560, 560), (1641026805000, 1, 561, 561, 561), (1641026810000, 1, 562, 562, 562), (1641026815000, 1, 563, 563, 563), (1641026820000, 1, 564, 564, 564), (1641026825000, 1, 565, 565, 565), (1641026830000, 1, 566, 566, 566), (1641026835000, 1, 567, 567, 567), (1641026840000, 1, 568, 568, 568), (1641026845000, 1, 569, 569, 569), (1641026850000, 1, 570, 570, 570), (1641026855000, 1, 571, 571, 571), (1641026860000, 1, 572, 572, 572), (1641026865000, 1, 573, 573, 573), (1641026870000, 1, 574, 574, 574), (1641026875000, 1, 575, 575, 575), (1641026880000, 1, 576, 576, 576), (1641026885000, 1, 577, 577, 577), (1641026890000, 1, 578, 578, 578), (1641026895000, 1, 579, 579, 579), (1641026900000, 1, 580, 580, 580), (1641026905000, 1, 581, 581, 581), (1641026910000, 1, 582, 582, 582), (1641026915000, 1, 583, 583, 583), (1641026920000, 1, 584, 584, 584), (1641026925000, 1, 585, 585, 585), (1641026930000, 1, 586, 586, 586), (1641026935000, 1, 587, 587, 587), (1641026940000, 1, 588, 588, 588), (1641026945000, 1, 589, 589, 589), (1641026950000, 1, 590, 590, 590), (1641026955000, 1, 591, 591, 591), (1641026960000, 1, 592, 592, 592), (1641026965000, 1, 593, 593, 593), (1641026970000, 1, 594, 594, 594), (1641026975000, 1, 595, 595, 595), (1641026980000, 1, 596, 596, 596), (1641026985000, 1, 597, 597, 597), (1641026990000, 1, 598, 598, 598), (1641026995000, 1, 599, 599, 599);\n" +
                "INSERT INTO t1(time, deviceid, c0, c1, c2) VALUES(1641027000000, 1, 600, 600, 600), (1641027005000, 1, 601, 601, 601), (1641027010000, 1, 602, 602, 602), (1641027015000, 1, 603, 603, 603), (1641027020000, 1, 604, 604, 604), (1641027025000, 1, 605, 605, 605), (1641027030000, 1, 606, 606, 606), (1641027035000, 1, 607, 607, 607), (1641027040000, 1, 608, 608, 608), (1641027045000, 1, 609, 609, 609), (1641027050000, 1, 610, 610, 610), (1641027055000, 1, 611, 611, 611), (1641027060000, 1, 612, 612, 612), (1641027065000, 1, 613, 613, 613), (1641027070000, 1, 614, 614, 614), (1641027075000, 1, 615, 615, 615), (1641027080000, 1, 616, 616, 616), (1641027085000, 1, 617, 617, 617), (1641027090000, 1, 618, 618, 618), (1641027095000, 1, 619, 619, 619), (1641027100000, 1, 620, 620, 620), (1641027105000, 1, 621, 621, 621), (1641027110000, 1, 622, 622, 622), (1641027115000, 1, 623, 623, 623), (1641027120000, 1, 624, 624, 624), (1641027125000, 1, 625, 625, 625), (1641027130000, 1, 626, 626, 626), (1641027135000, 1, 627, 627, 627), (1641027140000, 1, 628, 628, 628), (1641027145000, 1, 629, 629, 629), (1641027150000, 1, 630, 630, 630), (1641027155000, 1, 631, 631, 631), (1641027160000, 1, 632, 632, 632), (1641027165000, 1, 633, 633, 633), (1641027170000, 1, 634, 634, 634), (1641027175000, 1, 635, 635, 635), (1641027180000, 1, 636, 636, 636), (1641027185000, 1, 637, 637, 637), (1641027190000, 1, 638, 638, 638), (1641027195000, 1, 639, 639, 639), (1641027200000, 1, 640, 640, 640), (1641027205000, 1, 641, 641, 641), (1641027210000, 1, 642, 642, 642), (1641027215000, 1, 643, 643, 643), (1641027220000, 1, 644, 644, 644), (1641027225000, 1, 645, 645, 645), (1641027230000, 1, 646, 646, 646), (1641027235000, 1, 647, 647, 647), (1641027240000, 1, 648, 648, 648), (1641027245000, 1, 649, 649, 649), (1641027250000, 1, 650, 650, 650), (1641027255000, 1, 651, 651, 651), (1641027260000, 1, 652, 652, 652), (1641027265000, 1, 653, 653, 653), (1641027270000, 1, 654, 654, 654), (1641027275000, 1, 655, 655, 655), (1641027280000, 1, 656, 656, 656), (1641027285000, 1, 657, 657, 657), (1641027290000, 1, 658, 658, 658), (1641027295000, 1, 659, 659, 659), (1641027300000, 1, 660, 660, 660), (1641027305000, 1, 661, 661, 661), (1641027310000, 1, 662, 662, 662), (1641027315000, 1, 663, 663, 663), (1641027320000, 1, 664, 664, 664), (1641027325000, 1, 665, 665, 665), (1641027330000, 1, 666, 666, 666), (1641027335000, 1, 667, 667, 667), (1641027340000, 1, 668, 668, 668), (1641027345000, 1, 669, 669, 669), (1641027350000, 1, 670, 670, 670), (1641027355000, 1, 671, 671, 671), (1641027360000, 1, 672, 672, 672), (1641027365000, 1, 673, 673, 673), (1641027370000, 1, 674, 674, 674), (1641027375000, 1, 675, 675, 675), (1641027380000, 1, 676, 676, 676), (1641027385000, 1, 677, 677, 677), (1641027390000, 1, 678, 678, 678), (1641027395000, 1, 679, 679, 679), (1641027400000, 1, 680, 680, 680), (1641027405000, 1, 681, 681, 681), (1641027410000, 1, 682, 682, 682), (1641027415000, 1, 683, 683, 683), (1641027420000, 1, 684, 684, 684), (1641027425000, 1, 685, 685, 685), (1641027430000, 1, 686, 686, 686), (1641027435000, 1, 687, 687, 687), (1641027440000, 1, 688, 688, 688), (1641027445000, 1, 689, 689, 689), (1641027450000, 1, 690, 690, 690), (1641027455000, 1, 691, 691, 691), (1641027460000, 1, 692, 692, 692), (1641027465000, 1, 693, 693, 693), (1641027470000, 1, 694, 694, 694), (1641027475000, 1, 695, 695, 695), (1641027480000, 1, 696, 696, 696), (1641027485000, 1, 697, 697, 697), (1641027490000, 1, 698, 698, 698), (1641027495000, 1, 699, 699, 699);\n" +
                "INSERT INTO t1(time, deviceid, c0, c1, c2) VALUES(1641027500000, 1, 700, 700, 700), (1641027505000, 1, 701, 701, 701), (1641027510000, 1, 702, 702, 702), (1641027515000, 1, 703, 703, 703), (1641027520000, 1, 704, 704, 704), (1641027525000, 1, 705, 705, 705), (1641027530000, 1, 706, 706, 706), (1641027535000, 1, 707, 707, 707), (1641027540000, 1, 708, 708, 708), (1641027545000, 1, 709, 709, 709), (1641027550000, 1, 710, 710, 710), (1641027555000, 1, 711, 711, 711), (1641027560000, 1, 712, 712, 712), (1641027565000, 1, 713, 713, 713), (1641027570000, 1, 714, 714, 714), (1641027575000, 1, 715, 715, 715), (1641027580000, 1, 716, 716, 716), (1641027585000, 1, 717, 717, 717), (1641027590000, 1, 718, 718, 718), (1641027595000, 1, 719, 719, 719), (1641027600000, 1, 720, 720, 720), (1641027605000, 1, 721, 721, 721), (1641027610000, 1, 722, 722, 722), (1641027615000, 1, 723, 723, 723), (1641027620000, 1, 724, 724, 724), (1641027625000, 1, 725, 725, 725), (1641027630000, 1, 726, 726, 726), (1641027635000, 1, 727, 727, 727), (1641027640000, 1, 728, 728, 728), (1641027645000, 1, 729, 729, 729), (1641027650000, 1, 730, 730, 730), (1641027655000, 1, 731, 731, 731), (1641027660000, 1, 732, 732, 732), (1641027665000, 1, 733, 733, 733), (1641027670000, 1, 734, 734, 734), (1641027675000, 1, 735, 735, 735), (1641027680000, 1, 736, 736, 736), (1641027685000, 1, 737, 737, 737), (1641027690000, 1, 738, 738, 738), (1641027695000, 1, 739, 739, 739), (1641027700000, 1, 740, 740, 740), (1641027705000, 1, 741, 741, 741), (1641027710000, 1, 742, 742, 742), (1641027715000, 1, 743, 743, 743), (1641027720000, 1, 744, 744, 744), (1641027725000, 1, 745, 745, 745), (1641027730000, 1, 746, 746, 746), (1641027735000, 1, 747, 747, 747), (1641027740000, 1, 748, 748, 748), (1641027745000, 1, 749, 749, 749), (1641027750000, 1, 750, 750, 750), (1641027755000, 1, 751, 751, 751), (1641027760000, 1, 752, 752, 752), (1641027765000, 1, 753, 753, 753), (1641027770000, 1, 754, 754, 754), (1641027775000, 1, 755, 755, 755), (1641027780000, 1, 756, 756, 756), (1641027785000, 1, 757, 757, 757), (1641027790000, 1, 758, 758, 758), (1641027795000, 1, 759, 759, 759), (1641027800000, 1, 760, 760, 760), (1641027805000, 1, 761, 761, 761), (1641027810000, 1, 762, 762, 762), (1641027815000, 1, 763, 763, 763), (1641027820000, 1, 764, 764, 764), (1641027825000, 1, 765, 765, 765), (1641027830000, 1, 766, 766, 766), (1641027835000, 1, 767, 767, 767), (1641027840000, 1, 768, 768, 768), (1641027845000, 1, 769, 769, 769), (1641027850000, 1, 770, 770, 770), (1641027855000, 1, 771, 771, 771), (1641027860000, 1, 772, 772, 772), (1641027865000, 1, 773, 773, 773), (1641027870000, 1, 774, 774, 774), (1641027875000, 1, 775, 775, 775), (1641027880000, 1, 776, 776, 776), (1641027885000, 1, 777, 777, 777), (1641027890000, 1, 778, 778, 778), (1641027895000, 1, 779, 779, 779), (1641027900000, 1, 780, 780, 780), (1641027905000, 1, 781, 781, 781), (1641027910000, 1, 782, 782, 782), (1641027915000, 1, 783, 783, 783), (1641027920000, 1, 784, 784, 784), (1641027925000, 1, 785, 785, 785), (1641027930000, 1, 786, 786, 786), (1641027935000, 1, 787, 787, 787), (1641027940000, 1, 788, 788, 788), (1641027945000, 1, 789, 789, 789), (1641027950000, 1, 790, 790, 790), (1641027955000, 1, 791, 791, 791), (1641027960000, 1, 792, 792, 792), (1641027965000, 1, 793, 793, 793), (1641027970000, 1, 794, 794, 794), (1641027975000, 1, 795, 795, 795), (1641027980000, 1, 796, 796, 796), (1641027985000, 1, 797, 797, 797), (1641027990000, 1, 798, 798, 798), (1641027995000, 1, 799, 799, 799);\n" +
                "INSERT INTO t1(time, deviceid, c0, c1, c2) VALUES(1641028000000, 1, 800, 800, 800), (1641028005000, 1, 801, 801, 801), (1641028010000, 1, 802, 802, 802), (1641028015000, 1, 803, 803, 803), (1641028020000, 1, 804, 804, 804), (1641028025000, 1, 805, 805, 805), (1641028030000, 1, 806, 806, 806), (1641028035000, 1, 807, 807, 807), (1641028040000, 1, 808, 808, 808), (1641028045000, 1, 809, 809, 809), (1641028050000, 1, 810, 810, 810), (1641028055000, 1, 811, 811, 811), (1641028060000, 1, 812, 812, 812), (1641028065000, 1, 813, 813, 813), (1641028070000, 1, 814, 814, 814), (1641028075000, 1, 815, 815, 815), (1641028080000, 1, 816, 816, 816), (1641028085000, 1, 817, 817, 817), (1641028090000, 1, 818, 818, 818), (1641028095000, 1, 819, 819, 819), (1641028100000, 1, 820, 820, 820), (1641028105000, 1, 821, 821, 821), (1641028110000, 1, 822, 822, 822), (1641028115000, 1, 823, 823, 823), (1641028120000, 1, 824, 824, 824), (1641028125000, 1, 825, 825, 825), (1641028130000, 1, 826, 826, 826), (1641028135000, 1, 827, 827, 827), (1641028140000, 1, 828, 828, 828), (1641028145000, 1, 829, 829, 829), (1641028150000, 1, 830, 830, 830), (1641028155000, 1, 831, 831, 831), (1641028160000, 1, 832, 832, 832), (1641028165000, 1, 833, 833, 833), (1641028170000, 1, 834, 834, 834), (1641028175000, 1, 835, 835, 835), (1641028180000, 1, 836, 836, 836), (1641028185000, 1, 837, 837, 837), (1641028190000, 1, 838, 838, 838), (1641028195000, 1, 839, 839, 839), (1641028200000, 1, 840, 840, 840), (1641028205000, 1, 841, 841, 841), (1641028210000, 1, 842, 842, 842), (1641028215000, 1, 843, 843, 843), (1641028220000, 1, 844, 844, 844), (1641028225000, 1, 845, 845, 845), (1641028230000, 1, 846, 846, 846), (1641028235000, 1, 847, 847, 847), (1641028240000, 1, 848, 848, 848), (1641028245000, 1, 849, 849, 849), (1641028250000, 1, 850, 850, 850), (1641028255000, 1, 851, 851, 851), (1641028260000, 1, 852, 852, 852), (1641028265000, 1, 853, 853, 853), (1641028270000, 1, 854, 854, 854), (1641028275000, 1, 855, 855, 855), (1641028280000, 1, 856, 856, 856), (1641028285000, 1, 857, 857, 857), (1641028290000, 1, 858, 858, 858), (1641028295000, 1, 859, 859, 859), (1641028300000, 1, 860, 860, 860), (1641028305000, 1, 861, 861, 861), (1641028310000, 1, 862, 862, 862), (1641028315000, 1, 863, 863, 863), (1641028320000, 1, 864, 864, 864), (1641028325000, 1, 865, 865, 865), (1641028330000, 1, 866, 866, 866), (1641028335000, 1, 867, 867, 867), (1641028340000, 1, 868, 868, 868), (1641028345000, 1, 869, 869, 869), (1641028350000, 1, 870, 870, 870), (1641028355000, 1, 871, 871, 871), (1641028360000, 1, 872, 872, 872), (1641028365000, 1, 873, 873, 873), (1641028370000, 1, 874, 874, 874), (1641028375000, 1, 875, 875, 875), (1641028380000, 1, 876, 876, 876), (1641028385000, 1, 877, 877, 877), (1641028390000, 1, 878, 878, 878), (1641028395000, 1, 879, 879, 879), (1641028400000, 1, 880, 880, 880), (1641028405000, 1, 881, 881, 881), (1641028410000, 1, 882, 882, 882), (1641028415000, 1, 883, 883, 883), (1641028420000, 1, 884, 884, 884), (1641028425000, 1, 885, 885, 885), (1641028430000, 1, 886, 886, 886), (1641028435000, 1, 887, 887, 887), (1641028440000, 1, 888, 888, 888), (1641028445000, 1, 889, 889, 889), (1641028450000, 1, 890, 890, 890), (1641028455000, 1, 891, 891, 891), (1641028460000, 1, 892, 892, 892), (1641028465000, 1, 893, 893, 893), (1641028470000, 1, 894, 894, 894), (1641028475000, 1, 895, 895, 895), (1641028480000, 1, 896, 896, 896), (1641028485000, 1, 897, 897, 897), (1641028490000, 1, 898, 898, 898), (1641028495000, 1, 899, 899, 899);\n" +
                "INSERT INTO t1(time, deviceid, c0, c1, c2) VALUES(1641028500000, 1, 900, 900, 900), (1641028505000, 1, 901, 901, 901), (1641028510000, 1, 902, 902, 902), (1641028515000, 1, 903, 903, 903), (1641028520000, 1, 904, 904, 904), (1641028525000, 1, 905, 905, 905), (1641028530000, 1, 906, 906, 906), (1641028535000, 1, 907, 907, 907), (1641028540000, 1, 908, 908, 908), (1641028545000, 1, 909, 909, 909), (1641028550000, 1, 910, 910, 910), (1641028555000, 1, 911, 911, 911), (1641028560000, 1, 912, 912, 912), (1641028565000, 1, 913, 913, 913), (1641028570000, 1, 914, 914, 914), (1641028575000, 1, 915, 915, 915), (1641028580000, 1, 916, 916, 916), (1641028585000, 1, 917, 917, 917), (1641028590000, 1, 918, 918, 918), (1641028595000, 1, 919, 919, 919), (1641028600000, 1, 920, 920, 920), (1641028605000, 1, 921, 921, 921), (1641028610000, 1, 922, 922, 922), (1641028615000, 1, 923, 923, 923), (1641028620000, 1, 924, 924, 924), (1641028625000, 1, 925, 925, 925), (1641028630000, 1, 926, 926, 926), (1641028635000, 1, 927, 927, 927), (1641028640000, 1, 928, 928, 928), (1641028645000, 1, 929, 929, 929), (1641028650000, 1, 930, 930, 930), (1641028655000, 1, 931, 931, 931), (1641028660000, 1, 932, 932, 932), (1641028665000, 1, 933, 933, 933), (1641028670000, 1, 934, 934, 934), (1641028675000, 1, 935, 935, 935), (1641028680000, 1, 936, 936, 936), (1641028685000, 1, 937, 937, 937), (1641028690000, 1, 938, 938, 938), (1641028695000, 1, 939, 939, 939), (1641028700000, 1, 940, 940, 940), (1641028705000, 1, 941, 941, 941), (1641028710000, 1, 942, 942, 942), (1641028715000, 1, 943, 943, 943), (1641028720000, 1, 944, 944, 944), (1641028725000, 1, 945, 945, 945), (1641028730000, 1, 946, 946, 946), (1641028735000, 1, 947, 947, 947), (1641028740000, 1, 948, 948, 948), (1641028745000, 1, 949, 949, 949), (1641028750000, 1, 950, 950, 950), (1641028755000, 1, 951, 951, 951), (1641028760000, 1, 952, 952, 952), (1641028765000, 1, 953, 953, 953), (1641028770000, 1, 954, 954, 954), (1641028775000, 1, 955, 955, 955), (1641028780000, 1, 956, 956, 956), (1641028785000, 1, 957, 957, 957), (1641028790000, 1, 958, 958, 958), (1641028795000, 1, 959, 959, 959), (1641028800000, 1, 960, 960, 960), (1641028805000, 1, 961, 961, 961), (1641028810000, 1, 962, 962, 962), (1641028815000, 1, 963, 963, 963), (1641028820000, 1, 964, 964, 964), (1641028825000, 1, 965, 965, 965), (1641028830000, 1, 966, 966, 966), (1641028835000, 1, 967, 967, 967), (1641028840000, 1, 968, 968, 968), (1641028845000, 1, 969, 969, 969), (1641028850000, 1, 970, 970, 970), (1641028855000, 1, 971, 971, 971), (1641028860000, 1, 972, 972, 972), (1641028865000, 1, 973, 973, 973), (1641028870000, 1, 974, 974, 974), (1641028875000, 1, 975, 975, 975), (1641028880000, 1, 976, 976, 976), (1641028885000, 1, 977, 977, 977), (1641028890000, 1, 978, 978, 978), (1641028895000, 1, 979, 979, 979), (1641028900000, 1, 980, 980, 980), (1641028905000, 1, 981, 981, 981), (1641028910000, 1, 982, 982, 982), (1641028915000, 1, 983, 983, 983), (1641028920000, 1, 984, 984, 984), (1641028925000, 1, 985, 985, 985), (1641028930000, 1, 986, 986, 986), (1641028935000, 1, 987, 987, 987), (1641028940000, 1, 988, 988, 988), (1641028945000, 1, 989, 989, 989), (1641028950000, 1, 990, 990, 990), (1641028955000, 1, 991, 991, 991), (1641028960000, 1, 992, 992, 992), (1641028965000, 1, 993, 993, 993), (1641028970000, 1, 994, 994, 994), (1641028975000, 1, 995, 995, 995), (1641028980000, 1, 996, 996, 996), (1641028985000, 1, 997, 997, 997), (1641028990000, 1, 998, 998, 998), (1641028995000, 1, 999, 999, 999);\n";
        sql = sql.replace(";\nINSERT INTO t1(time, deviceid, c0, c1, c2) VALUES", ", ");
        executeSQL(sql);
    }

    @Test
    public void testQuadraticEquations() {
        Equations equations = new Equations(Equations.EquationType.quadraticEquation,
                Arrays.asList(BigDecimal.ONE, BigDecimal.ZERO));

//        log.info("{}", equations.genValueByTimestamp(1641024000000L));
//        log.info("{}", equations.genValueByTimestamp(1641024005000L));

        RangeConstraint rangeConstraint = new RangeConstraint(BigDecimal.ONE, BigDecimal.TEN);
        TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint("TEST", rangeConstraint);
        timeSeriesConstraint.addNotEqualValue(BigDecimal.ONE);
        timeSeriesConstraint.addNotEqualValue(BigDecimal.ONE.negate());
        timeSeriesConstraint.addNotEqualValue(BigDecimal.ZERO);
        timeSeriesConstraint.getRangeConstraints().add(new RangeConstraint(BigDecimal.TEN.negate(), BigDecimal.ONE.negate()));
        timeSeriesConstraint.getRangeConstraints().add(new RangeConstraint(BigDecimal.ONE.negate(), BigDecimal.ONE));

        log.info("{}", equations.transformTimeSeriesConstraint(timeSeriesConstraint));
    }

    @Test
    public void getSamplingFrequency() {
        SamplingFrequencyManager.getInstance().addSamplingFrequency("d", "t",
                1641024000000L, 30L * 5000L, 30L);
        SamplingFrequency samplingFrequency = SamplingFrequencyManager.getInstance()
                .getSamplingFrequencyFromCollection("d", "t");
//        List<Long> timestamps = samplingFrequency.apply(1641024000000L, 1641024150000L);
        List<Long> timestamps = samplingFrequency.apply(1641024000000L, 1641024450000L);
        log.info("{}", timestamps);

        List<Long> seqList = new ArrayList<>();
        for (int i = 0; i < timestamps.size(); i++) {
            seqList.add(samplingFrequency.getSeqByTimestamp(timestamps.get(i)).longValue());
        }
        log.info("{}", seqList);

        ConstraintValue.TimeSeriesConstraintValue constraintValue =
                new ConstraintValue.TimeSeriesConstraintValue(ConstraintValueGenerator.genBaseTimeSeries(),
                        new TimeSeriesConstraint("t"));
        constraintValue.getTimeSeriesConstraint().getNotEqualValues().addAll(
                Arrays.asList(BigDecimal.valueOf(1641024033000L), BigDecimal.valueOf(1641024058000L)));
        constraintValue.getTimeSeriesConstraint().setRangeConstraints(
                Arrays.asList(new RangeConstraint(BigDecimal.valueOf(1641024011000L), BigDecimal.valueOf(1641024058200L))));
        log.info("{}", samplingFrequency.transformToBaseSeqConstraintValue(constraintValue).getTimeSeriesConstraint());
    }

}
