package com.tsFuzzy.tsdbms.tdengine;

import com.fuzzy.Main;
import com.fuzzy.common.constant.GlobalConstant;
import com.tsFuzzy.tsdbms.TestConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@Slf4j
public class TestTDengine {

    @Test
    public void testPQS() {
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--drop-database", "--max-expression-depth", "4",
                        "--log-execution-time", "false",
                        "--num-tries", "2000",
                        "--random-string-generation", "ALPHANUMERIC", "--log-syntax-error-query", "true",
                        "--host", "172.29.185.200", "--port", "6041", "--username", "root", "--password", "taosdata",
                        "--database-prefix", "pqsdb", "--max-generated-databases", "1", "--start-timestamp", "1641024000",
                        "--precision", "ms",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.TDENGINE_DATABASE_NAME, "--oracle", "PQS"}));
    }

    @Test
    public void testTemplateQS() {
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--max-expression-depth", "4", "--drop-database",
                        "--random-string-generation", "ALPHANUMERIC", "--log-syntax-error-query", "true",
                        "--host", "localhost", "--port", "6041", "--username", "root", "--password", "taosdata",
                        "--database-prefix", "pqsdb", "--max-generated-databases", "1", "--start-timestamp", "1641024000",
                        "--precision", "ms",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.TDENGINE_DATABASE_NAME, "--oracle", "TemplateQS"}));
    }

    @Test
    public void testTSAF() {
        // "--drop-database",
        // "--use-syntax-sequence"
        // "--use-syntax-validator"
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--max-expression-depth", "4", "--drop-database",
                        "--num-tries", "2000", "--use-syntax-sequence", "--use-syntax-validator",
                        "--log-execution-time", "false",
                        "--random-string-generation", "ALPHANUMERIC", "--log-syntax-error-query", "true",
                        "--host", "172.29.185.200", "--port", "6041", "--username", "root", "--password", "taosdata",
                        "--database-prefix", "tsafdb", "--max-generated-databases", "1", "--start-timestamp", "1641024000",
                        "--precision", "ms",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.TDENGINE_DATABASE_NAME, "--oracle", "TSAF"}));
    }

    @Test
    public void testTSAFWithoutS() {
        // "--drop-database",
        // "--use-syntax-validator"
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--max-expression-depth", "4", "--drop-database",
                        "--num-tries", "2000", "--use-syntax-validator",
                        "--log-execution-time", "false",
                        "--random-string-generation", "ALPHANUMERIC", "--log-syntax-error-query", "true",
                        "--host", "172.29.185.200", "--port", "6041", "--username", "root", "--password", "taosdata",
                        "--database-prefix", "tsafdb", "--max-generated-databases", "1", "--start-timestamp", "1641024000",
                        "--precision", "ms",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.TDENGINE_DATABASE_NAME, "--oracle", "TSAF"}));
    }

    @Test
    public void testTSAFWithoutC() {
        // "--drop-database",
        // "--use-syntax-sequence"
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--max-expression-depth", "4", "--drop-database",
                        "--num-tries", "2000", "--use-syntax-sequence",
                        "--log-execution-time", "false",
                        "--random-string-generation", "ALPHANUMERIC", "--log-syntax-error-query", "true",
                        "--host", "172.29.185.200", "--port", "6041", "--username", "root", "--password", "taosdata",
                        "--database-prefix", "tsafdb", "--max-generated-databases", "1", "--start-timestamp", "1641024000",
                        "--precision", "ms",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.TDENGINE_DATABASE_NAME, "--oracle", "TSAF"}));
    }

    @Test
    public void testTSAFWithoutAll() {
        // "--drop-database",
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--max-expression-depth", "4", "--drop-database",
                        "--num-tries", "2000",
                        "--log-execution-time", "false",
                        "--random-string-generation", "ALPHANUMERIC", "--log-syntax-error-query", "true",
                        "--host", "172.29.185.200", "--port", "6041", "--username", "root", "--password", "taosdata",
                        "--database-prefix", "tsafdb", "--max-generated-databases", "1", "--start-timestamp", "1641024000",
                        "--precision", "ms",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.TDENGINE_DATABASE_NAME, "--oracle", "TSAF"}));
    }
}
