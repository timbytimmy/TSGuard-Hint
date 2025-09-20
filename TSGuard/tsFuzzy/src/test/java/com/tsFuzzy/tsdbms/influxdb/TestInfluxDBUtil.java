package com.tsFuzzy.tsdbms.influxdb;

import com.alibaba.fastjson.JSONObject;
import com.fuzzy.common.util.TimeUtil;
import com.fuzzy.influxdb.InfluxDBConnection;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBColumn;
import com.fuzzy.influxdb.InfluxDBSchema.InfluxDBDataType;
import com.fuzzy.influxdb.InfluxDBStatement;
import com.fuzzy.influxdb.InfluxDBVisitor;
import com.fuzzy.influxdb.ast.*;
import com.fuzzy.influxdb.ast.InfluxDBBinaryLogicalOperation.InfluxDBBinaryLogicalOperator;
import com.fuzzy.influxdb.ast.InfluxDBCastOperation.CastType;
import com.fuzzy.influxdb.ast.InfluxDBConstant.InfluxDBIntConstant;
import com.fuzzy.influxdb.ast.InfluxDBUnaryNotPrefixOperation.InfluxDBUnaryNotPrefixOperator;
import com.fuzzy.influxdb.ast.InfluxDBUnaryPrefixOperation.InfluxDBUnaryPrefixOperator;
import com.fuzzy.influxdb.constant.InfluxDBPrecision;
import com.fuzzy.influxdb.resultSet.InfluxDBResultSet;
import com.fuzzy.influxdb.resultSet.InfluxDBSeries;
import com.fuzzy.influxdb.util.InfluxDBAuthorizationParams;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class TestInfluxDBUtil {
    private static final String host = "172.29.185.200";
    private static final int port = 8086;
    private static final String token = "pCGSL9nyhswIxaOjZnd59NGHxiFEh9yrtOViWqc4W8062eF1SHRbBoA-NlrFAgUOo5KuB6Bq3hRWNsuet6AdRQ==";
    private static final String orgId = "e8a8738626052670";
    private static final String bucket = "bucket";
    private static final String precision = InfluxDBPrecision.s.toString();

    @Test
    public void testGetRandomPrecision() {
        System.out.println(InfluxDBPrecision.getRandom());
    }

    @Test
    public void testGetCharAtIndex() {
        int index = 111;
        String str = "q=SELECT t1_f0 AS ref0, t1_tag0 AS ref1 FROM pqsdb0.autogen.t1 WHERE (TRUE != (((- ((80311027) ^ (3777737359)))) & (('' = ('')))))";
        log.info("char:{}{}{}{}", str.charAt(index - 4), str.charAt(index - 3), str.charAt(index - 2), str.charAt(index - 1));
    }

    @Test
    public void tsst() {
        InfluxDBAuthorizationParams authorizationParams = new InfluxDBAuthorizationParams();
        authorizationParams.setOrganizationId("abc");
        authorizationParams.setToken("token");
        log.info("{}", JSONObject.toJSONString(authorizationParams));
    }

    @Test
    public void testSelectVisitor() {
        Arrays.stream(InfluxDBUnaryPrefixOperation.InfluxDBUnaryPrefixOperator.values())
                .filter(e -> e != InfluxDBUnaryPrefixOperator.MINUS && e != InfluxDBUnaryPrefixOperator.PLUS)
                .collect(Collectors.toList()).forEach(e -> log.info("{}", e));
    }

    @Test
    public void genOperation() {
        String selectCause = "select * from db0.autogen.t0 WHERE";
//        InfluxDBIntConstant subExpr = new InfluxDBIntConstant(100, false, true);
//        InfluxDBConstant subExpr = InfluxDBConstant.createSingleQuotesStringConstant("truefalse");
//        InfluxDBConstant subExpr = InfluxDBConstant.createBoolean(true);
        InfluxDBColumnReference columnReference = new InfluxDBColumnReference(
                new InfluxDBColumn("name", false, InfluxDBDataType.STRING),
                InfluxDBConstant.createSingleQuotesStringConstant("truefalse"));
        InfluxDBCastOperation castOperation = new InfluxDBCastOperation(columnReference, CastType.FIELD);
        InfluxDBUnaryNotPrefixOperation notPrefixOperation = new InfluxDBUnaryNotPrefixOperation(castOperation,
                InfluxDBUnaryNotPrefixOperator.NOT_STR);
        log.info(String.format("%s %s", selectCause,
                InfluxDBVisitor.asString(notPrefixOperation)));
//        log.info(String.format("%s %s", selectCause,
//                        InfluxDBVisitor.asString(subExpr)));
    }

    @Test
    public void testSqlStatement() throws SQLException {
        InfluxDBConnection influxDBConnection = new InfluxDBConnection(host, port, token, orgId, bucket,
                precision);

        Long a = -1011557348L;
        Long b = 910552510L;
        log.info("{}", a | b);
        StringBuilder query = new StringBuilder();
        query.append("q=")// 0.0017195642316885744 0.00171961
                .append("SHOW MEASUREMENTS ON pqsdb0 WITH MEASUREMENT = \"pqsdb0\" WHERE t2_tag0 != ''")
//                .append("&epoch=ms")
        ;
        try (InfluxDBStatement s = influxDBConnection.createStatement()) {
            InfluxDBResultSet influxDBResultSet = (InfluxDBResultSet) s.executeQuery(query.toString());
            printSqlQueryResult(influxDBResultSet);
        }
    }

    @Test
    public void dropDatabases() throws SQLException {
        InfluxDBConnection influxDBConnection = new InfluxDBConnection(host, port, token, orgId, bucket,
                precision);
        String query = "q=SHOW DATABASES";

        try (InfluxDBStatement s = influxDBConnection.createStatement()) {
            InfluxDBResultSet influxDBResultSet = (InfluxDBResultSet) s.executeQuery(query);
            for (int i = 0; i < influxDBResultSet.getSeriesList().size(); i++) {
                InfluxDBSeries influxDBSeries = influxDBResultSet.getSeriesList().get(i);
                for (int rowValueIndex = 0; rowValueIndex < influxDBSeries.getValues().size(); rowValueIndex++) {
                    List<String> rowValues = influxDBSeries.getValues().get(rowValueIndex);
                    StringBuilder columnValue = new StringBuilder();
                    for (int columnValueIndex = 0; columnValueIndex < rowValues.size(); columnValueIndex++) {
                        columnValue.append(rowValues.get(columnValueIndex));
                    }

                    String databaseName = columnValue.toString();
                    if (databaseName.startsWith("tsafdb") || databaseName.startsWith("pqsdb")) {
                        String dropDatabaseSql = String.format("q=DROP DATABASE %s", databaseName);
                        s.executeQuery(dropDatabaseSql);
                        log.info("DROP DATABASE: {}", databaseName);
                    }
                }
            }
        }
    }

    private void printSqlQueryResult(InfluxDBResultSet influxDBResultSet) {
        log.info("statementId: {}", influxDBResultSet.getStatementId());
        for (int i = 0; i < influxDBResultSet.getSeriesList().size(); i++) {
            InfluxDBSeries influxDBSeries = influxDBResultSet.getSeriesList().get(i);
            log.info("name: {}", influxDBSeries.getName());
            StringBuilder columnInfo = new StringBuilder();
            for (int columnIndex = 0; columnIndex < influxDBSeries.getColumns().size(); columnIndex++) {
                columnInfo.append(influxDBSeries.getColumns().get(columnIndex)).append("\t").append("\t");
            }
            log.info("{}", columnInfo);

            for (int rowValueIndex = 0; rowValueIndex < influxDBSeries.getValues().size(); rowValueIndex++) {
                List<String> rowValues = influxDBSeries.getValues().get(rowValueIndex);
                StringBuilder columnValue = new StringBuilder();
                for (int columnValueIndex = 0; columnValueIndex < rowValues.size(); columnValueIndex++) {
                    columnValue.append(rowValues.get(columnValueIndex)).append("\t").append("\t");
                }
                log.info("{}", columnValue);
            }
        }
    }

    @Test
    public void testVisitorAsExpectedValue() {
        InfluxDBColumnReference left = new InfluxDBColumnReference(new InfluxDBColumn("t0_f0", false,
                InfluxDBDataType.UINT), new InfluxDBIntConstant(1, false, true));
        InfluxDBConstant right = InfluxDBConstant.createSingleQuotesStringConstant("");
        InfluxDBBinaryLogicalOperation operator = new InfluxDBBinaryLogicalOperation(left, right, InfluxDBBinaryLogicalOperator.AND);
        InfluxDBVisitor.asExpectedValues(operator);
    }

    @Test
    public void testInsertRowData() throws Exception {
        InfluxDBConnection influxDBConnection = new InfluxDBConnection(host, port, token, orgId, bucket,
                precision);

        StringBuilder body = new StringBuilder();
        body.append("t4,t1_tag0=initTag t1_f0=\"string\" 1641024000");
        try (InfluxDBStatement s = influxDBConnection.createStatement()) {
            s.execute(body.toString());
        }
    }

    @Test
    public void testQuery() throws Exception {
//        InfluxDBExpression leftLogic = InfluxDBConstant.createDoubleConstant(1955418182);
//        InfluxDBExpression rightLogic = new InfluxDBColumnReference(new InfluxDBColumn("t1_tag0", true, null),
//                new InfluxDBConstant.InfluxDBTextConstant("initTag"));
//        InfluxDBExpression leftComparison = new InfluxDBBinaryLogicalOperation(leftLogic, rightLogic, InfluxDBBinaryLogicalOperator.OR);
//
//        InfluxDBExpression leftLogic1 = new InfluxDBColumnReference(new InfluxDBColumn("t1_f0", false, null),
//                new InfluxDBConstant.InfluxDBDoubleConstant(90.70016479));
//        InfluxDBExpression rightLogic1 = new InfluxDBColumnReference(new InfluxDBColumn("t1_f0", false, null),
//                new InfluxDBConstant.InfluxDBDoubleConstant(23.55579185));
//        InfluxDBExpression rightComparison = new InfluxDBBinaryLogicalOperation(leftLogic, rightLogic, InfluxDBBinaryLogicalOperator.AND);
        InfluxDBExpression expression = new InfluxDBBinaryComparisonOperation(
                InfluxDBConstant.createIntConstant(0, false),
                InfluxDBConstant.createDoubleConstant(-0),
                InfluxDBBinaryComparisonOperation.BinaryComparisonOperator.NOT_EQUALS);

        InfluxDBExpression notExpression = new InfluxDBUnaryNotPrefixOperation(
                expression, InfluxDBUnaryNotPrefixOperator.NOT_UINTEGER);

        InfluxDBConstant expected = expression.getExpectedValue();
    }

    @Test
    public void testMod() {
        log.info("{}", TimeUtil.timestampToRFC3339(new Date().getTime() / 1000));
    }

}
