package com.fuzzy.griddb;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fuzzy.DBMSSpecificOptions;
import com.fuzzy.OracleFactory;
import com.fuzzy.common.oracle.TestOracle;
import com.fuzzy.griddb.oracle.GridDBPivotedQuerySynthesisOracle;
import com.fuzzy.griddb.oracle.GridDBTSAFOracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "GridDB (default port: " + GridDBOptions.DEFAULT_PORT
        + ", default host: " + GridDBOptions.DEFAULT_HOST + ")")
public class GridDBOptions implements DBMSSpecificOptions<GridDBOptions.GridDBOracleFactory> {
    public static final String DEFAULT_HOST = "127.0.0.1";
    public static final int DEFAULT_PORT = 20001;

    @Parameter(names = "--oracle")
    public List<GridDBOracleFactory> oracles = Arrays.asList(GridDBOracleFactory.TSAF);

    public enum GridDBOracleFactory implements OracleFactory<GridDBGlobalState> {

        PQS {
            @Override
            public TestOracle<GridDBGlobalState> create(GridDBGlobalState globalState) throws SQLException {
                return new GridDBPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        },
        TSAF {
            @Override
            public TestOracle<GridDBGlobalState> create(GridDBGlobalState globalState) throws SQLException {
                return new GridDBTSAFOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        };
    }

    @Override
    public List<GridDBOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
