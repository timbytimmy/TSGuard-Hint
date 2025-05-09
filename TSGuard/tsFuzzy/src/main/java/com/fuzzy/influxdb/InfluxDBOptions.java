package com.fuzzy.influxdb;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fuzzy.DBMSSpecificOptions;
import com.fuzzy.OracleFactory;
import com.fuzzy.common.oracle.TestOracle;
import com.fuzzy.influxdb.InfluxDBOptions.InfluxDBOracleFactory;
import com.fuzzy.influxdb.oracle.InfluxDBPivotedQuerySynthesisOracle;
import com.fuzzy.influxdb.oracle.InfluxDBTSAFOracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "InfluxDB (default port: " + InfluxDBOptions.DEFAULT_PORT
        + ", default host: " + InfluxDBOptions.DEFAULT_HOST + ")")
public class InfluxDBOptions implements DBMSSpecificOptions<InfluxDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 8086;

    @Parameter(names = "--oracle")
    public List<InfluxDBOracleFactory> oracles = Arrays.asList(InfluxDBOracleFactory.TSAF);

    @Override
    public List<InfluxDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

    public enum InfluxDBOracleFactory implements OracleFactory<InfluxDBGlobalState> {
        PQS {
            @Override
            public TestOracle<InfluxDBGlobalState> create(InfluxDBGlobalState globalState) throws SQLException {
                return new InfluxDBPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }

        },
        TSAF {
            @Override
            public TestOracle<InfluxDBGlobalState> create(InfluxDBGlobalState globalState) throws SQLException {
                return new InfluxDBTSAFOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        };
    }
}
