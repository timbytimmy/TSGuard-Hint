package com.fuzzy.iotdb;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fuzzy.DBMSSpecificOptions;
import com.fuzzy.OracleFactory;
import com.fuzzy.common.oracle.TestOracle;
import com.fuzzy.iotdb.IotDBOptions.IotDBOracleFactory;
import com.fuzzy.iotdb.oracle.IotDBPivotedQuerySynthesisOracle;
import com.fuzzy.iotdb.oracle.IotDBTSAFOracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "IotDB (default port: " + IotDBOptions.DEFAULT_PORT
        + ", default host: " + IotDBOptions.DEFAULT_HOST + ")")
public class IotDBOptions implements DBMSSpecificOptions<IotDBOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 6667;

    @Parameter(names = "--oracle")
    public List<IotDBOracleFactory> oracles = Arrays.asList(IotDBOracleFactory.TSAF);

    public enum IotDBOracleFactory implements OracleFactory<IotDBGlobalState> {

        PQS {
            @Override
            public TestOracle<IotDBGlobalState> create(IotDBGlobalState globalState) throws SQLException {
                return new IotDBPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }

        },
        TSAF {
            @Override
            public TestOracle<IotDBGlobalState> create(IotDBGlobalState globalState) throws SQLException {
                return new IotDBTSAFOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        };
    }

    @Override
    public List<IotDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
