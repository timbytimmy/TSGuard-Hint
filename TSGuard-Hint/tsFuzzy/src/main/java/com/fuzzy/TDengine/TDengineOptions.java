package com.fuzzy.TDengine;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fuzzy.DBMSSpecificOptions;
import com.fuzzy.OracleFactory;
import com.fuzzy.TDengine.TDengineOptions.TDengineOracleFactory;
import com.fuzzy.TDengine.oracle.TDenginePivotedQuerySynthesisOracle;
import com.fuzzy.TDengine.oracle.TDengineTSAFOracle;
import com.fuzzy.TDengine.oracle.TDengineTemplateQuerySynthesisOracle;
import com.fuzzy.common.oracle.TestOracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "TDengine (default port: " + TDengineOptions.DEFAULT_PORT
        + ", default host: " + TDengineOptions.DEFAULT_HOST + ")")
public class TDengineOptions implements DBMSSpecificOptions<TDengineOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 6041;

    @Parameter(names = "--oracle")
    public List<TDengineOracleFactory> oracles = Arrays.asList(TDengineOracleFactory.TSAF);

    public enum TDengineOracleFactory implements OracleFactory<TDengineGlobalState> {

        PQS {
            @Override
            public TestOracle<TDengineGlobalState> create(TDengineGlobalState globalState) throws SQLException {
                return new TDenginePivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }

        },
        TemplateQS {
            @Override
            public TestOracle<TDengineGlobalState> create(TDengineGlobalState globalState) throws SQLException {
                return new TDengineTemplateQuerySynthesisOracle(globalState);
            }
        },
        TSAF {
            @Override
            public TestOracle<TDengineGlobalState> create(TDengineGlobalState globalState) throws SQLException {
                return new TDengineTSAFOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }
        };
    }

    @Override
    public List<TDengineOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
