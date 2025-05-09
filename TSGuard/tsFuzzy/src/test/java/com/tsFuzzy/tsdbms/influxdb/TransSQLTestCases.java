package com.tsFuzzy.tsdbms.influxdb;

import com.fuzzy.common.coverage.TransSQLTestCaseUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

@Slf4j
public class TransSQLTestCases {
    private static final String sourceSQLFilePath = "E:\\work project\\t3-tsms\\tsFuzzy\\logs\\influxdb";
    private static final String targetSQLFilePath = "E:\\work project\\t3-tsms\\tsFuzzy\\logs\\influxdb\\query.rs";

    @Test
    public void getSamplingFrequency() throws Exception {
        TransSQLTestCaseUtils.transSQLTestCaseInDirForInfluxDB(sourceSQLFilePath, targetSQLFilePath);
    }
}
