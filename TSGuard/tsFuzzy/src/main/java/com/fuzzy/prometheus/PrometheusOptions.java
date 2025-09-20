package com.fuzzy.prometheus;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fuzzy.DBMSSpecificOptions;
import com.fuzzy.OracleFactory;
import com.fuzzy.common.oracle.TestOracle;
import com.fuzzy.prometheus.PrometheusOptions.PrometheusOracleFactory;
import com.fuzzy.prometheus.oracle.PrometheusPivotedQuerySynthesisOracle;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "=", commandDescription = "Prometheus (default port: " + PrometheusOptions.DEFAULT_PORT
        + ", default host: " + PrometheusOptions.DEFAULT_HOST + ")")
public class PrometheusOptions implements DBMSSpecificOptions<PrometheusOracleFactory> {
    public static final String DEFAULT_HOST = "localhost";
    public static final int DEFAULT_PORT = 9990;

    @Parameter(names = "--oracle")
    public List<PrometheusOracleFactory> oracles = Arrays.asList(PrometheusOracleFactory.PQS);

    public enum PrometheusOracleFactory implements OracleFactory<PrometheusGlobalState> {

        PQS {

            @Override
            public TestOracle<PrometheusGlobalState> create(PrometheusGlobalState globalState) throws SQLException {
                return new PrometheusPivotedQuerySynthesisOracle(globalState);
            }

            @Override
            public boolean requiresAllTablesToContainRows() {
                return true;
            }

        };
    }

    @Override
    public List<PrometheusOracleFactory> getTestOracleFactory() {
        return oracles;
    }

}
