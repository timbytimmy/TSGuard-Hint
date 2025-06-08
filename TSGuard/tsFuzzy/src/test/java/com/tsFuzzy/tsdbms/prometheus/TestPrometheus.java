package com.tsFuzzy.tsdbms.prometheus;

import com.fuzzy.Main;
import com.fuzzy.common.constant.GlobalConstant;
import com.tsFuzzy.tsdbms.TestConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestPrometheus {

    @Test
    public void testPQS() {
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--random-string-generation", "ALPHANUMERIC",
                        "--host", "111.229.183.22", "--port", "9990",
                        "--database-prefix", "pqsdb", "--max-generated-databases", "1",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.IOTDB_DATABASE_NAME, "--oracle", "PQS"}));
    }

    @Test
    public void testTSAF() {
        // 测试数据时间范围：[cur - 3400, cur], unit: s
        long curTimestamp = System.currentTimeMillis() / 1000;
        long startTimestamp = curTimestamp - 3400;

        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--host", "172.29.185.200", "--port", "9090", "--precision", "ms",
                        "--log-syntax-error-query", "true", "--max-expression-depth", "4",
                        "--log-execution-time", "false", "--num-tries", "2000",
                        "--start-timestamp", String.valueOf(startTimestamp),
                        "--params", "",
                        "--use-syntax-validator", "--use-syntax-sequence",
                        "--random-string-generation", "ALPHANUMERIC", "--database-prefix",
                        "tsafdb", "--max-generated-databases", "1",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.PROMETHEUS_DATABASE_NAME, "--oracle", "TSAF"}));
    }
}
