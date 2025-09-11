package com.tsFuzzy.tsdbms.griddb;

import com.fuzzy.Randomly;
import com.fuzzy.common.tsaf.RangeConstraint;
import com.fuzzy.common.tsaf.TimeSeriesConstraint;
import com.fuzzy.griddb.GridDBSchema;
import com.fuzzy.griddb.GridDBVisitor;
import com.fuzzy.griddb.ast.GridDBBetweenOperation;
import com.fuzzy.griddb.ast.GridDBBinaryArithmeticOperation;
import com.fuzzy.griddb.ast.GridDBColumnReference;
import com.fuzzy.griddb.ast.GridDBConstant;
import com.fuzzy.griddb.tsaf.enu.GridDBConstantString;
import com.toshiba.mwcloud.gs.GSException;
import com.toshiba.mwcloud.gs.GridStore;
import com.toshiba.mwcloud.gs.GridStoreFactory;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

@Slf4j
public class TestGridDBUtil {

    public static String host = "127.0.0.1";
    public static int port = 20001;
    public static String clusterName = "myCluster";
    public static String databaseName = "public";
    public static String userName = "admin";
    public static String password = "admin";
    public static String url;

    static {
        // url format "jdbc:gs://(multicastAddress):(portNo)/(clusterName)"
        url = String.format("jdbc:gs://%s:%d/%s/%s", host, port, clusterName, databaseName);
    }

    public static void main(String[] args) throws SQLException {
        // Connection to a GridDB cluster
        Connection con = DriverManager.getConnection(url, userName, password);
        try {
            Statement st = con.createStatement();
//            DatabaseMetaData metaData = con.getMetaData();
//            ResultSet tables = metaData.getTables(null, null, null, new String[]{"TABLE"});
            ResultSet rs = st.executeQuery("SELECT ALL COUNT(c0), time FROM tsqsdb3_t0");
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                for (int i = 0; i < md.getColumnCount(); i++) {
                    System.out.print(rs.getString(i + 1) + "|");
                }
                System.out.println("");
            }
            rs.close();
            System.out.println("End");
            st.close();
        } finally {
            System.out.println("DB Connection Close");
            con.close();
        }
    }

    @Test
    public void createDatabase() throws GSException {
        Properties props = new Properties();
        props.setProperty("notificationMember", "127.0.0.1:10001");
        props.setProperty("clusterName", "myCluster");
        props.setProperty("user", "admin");
        props.setProperty("password", "admin");
        GridStore store = GridStoreFactory.getInstance().getGridStore(props);
    }

    @Test
    public void testConstraint() {
        // ((-0.003847 <= ((c0) * (0.006153)) AND ((c0) * (0.006153)) <= 0.016153)) AND ((time) BETWEEN (time) AND (time))
        GridDBColumnReference timeReference = new GridDBColumnReference(
                new GridDBSchema.GridDBColumn(GridDBConstantString.TIME_FIELD_NAME.getName(),
                        GridDBSchema.GridDBDataType.LONG), GridDBConstant.createInt64Constant(1));
        GridDBColumnReference columnReference = new GridDBColumnReference(
                new GridDBSchema.GridDBColumn("c0", GridDBSchema.GridDBDataType.LONG),
                GridDBConstant.createInt64Constant(1));
        GridDBColumnReference columnReference2 = new GridDBColumnReference(
                new GridDBSchema.GridDBColumn("c1", GridDBSchema.GridDBDataType.DOUBLE),
                GridDBConstant.createDoubleConstant(2));
//        Set<Long> nullValues = new HashSet<>(Arrays.asList(1L, 3L, 5L, 7L, 9L));
        Set<Long> nullValues = new HashSet<>();

        GridDBConstant constant1 = GridDBConstant.createTimestamp(7L);
        GridDBConstant constant2 = GridDBConstant.createTimestamp(6L);
        GridDBConstant constant3 = GridDBConstant.createInt64Constant(-1905613291L);
        GridDBConstant constant4 = GridDBConstant.createInt64Constant(-61930048L);
        GridDBConstant constant5 = GridDBConstant.createDoubleConstant(0.3505);
        GridDBConstant constant6 = GridDBConstant.createDoubleConstant(-0.981052);
        GridDBConstant constant7 = GridDBConstant.createDoubleConstant(1.060845);

        GridDBBinaryArithmeticOperation arithmeticOperation = new GridDBBinaryArithmeticOperation(columnReference,
                columnReference2, GridDBBinaryArithmeticOperation.GridDBBinaryArithmeticOperator.PLUS);
        GridDBBetweenOperation betweenOperation = new GridDBBetweenOperation(arithmeticOperation,
                constant3, constant5, false);

        TimeSeriesConstraint res = GridDBVisitor.asConstraint("databaseName", "tableName",
                betweenOperation, nullValues);
        log.info("{}", res);

        TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint("",
                new RangeConstraint(BigDecimal.valueOf(0.0180506), BigDecimal.valueOf(0.0819128)));
        log.info("{}", timeSeriesConstraint.valueInRange(BigDecimal.ONE, BigDecimal.ONE));
    }

    @Test
    public void testInRange() {
        Randomly randomly = new Randomly();
        for (int i = 0; i < 1000000; i++) {
            BigDecimal a = BigDecimal.valueOf(randomly.getInfiniteDouble());
            BigDecimal b = BigDecimal.valueOf(randomly.getInfiniteDouble());
            BigDecimal c = BigDecimal.valueOf(randomly.getInfiniteDouble());
            TimeSeriesConstraint timeSeriesConstraint = new TimeSeriesConstraint("",
                    new RangeConstraint(a, b));
            boolean compareRes = timeSeriesConstraint.valueInRange(c, BigDecimal.ONE);
            if ((c.compareTo(a) >= 0 && c.compareTo(b) <= 0) != compareRes) {
                log.error("{} {} {}", a, b, c);
            }
        }
    }
}
