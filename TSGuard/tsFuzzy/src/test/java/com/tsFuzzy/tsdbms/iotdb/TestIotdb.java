package com.tsFuzzy.tsdbms.iotdb;

import com.fuzzy.Main;
import com.fuzzy.common.constant.GlobalConstant;
import com.tsFuzzy.tsdbms.TestConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestIotdb {

    @Test
    public void testPQS() {
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--log-syntax-error-query", "true", "--drop-database",
                        "--num-tries", "2000", "--max-expression-depth", "4",
                        "--num-threads", "1", "--random-string-generation", "ALPHANUMERIC",
                        "--host", "172.29.185.200", "--port", "6667", "--username", "root", "--password", "root",
                        "--database-prefix", "root.", "--max-generated-databases", "1",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.IOTDB_DATABASE_NAME, "--oracle", "PQS"}));
    }

    @Test
    public void testTSAF() {
        // "--drop-database",
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--log-syntax-error-query", "true", "--drop-database", "--max-expression-depth", "4",
                        "--num-tries", "2000", "--use-syntax-validator", "--use-syntax-sequence",
                        "--num-threads", "1", "--random-string-generation", "ALPHANUMERIC",
                        "--host", "172.29.185.200", "--port", "6667", "--username", "root", "--password", "root",
                        "--database-prefix", "root.", "--max-generated-databases", "1",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.IOTDB_DATABASE_NAME, "--oracle", "TSAF"}));
    }

    @Test
    public void testTSAFWithoutC() {
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--log-syntax-error-query", "true", "--drop-database", "--max-expression-depth", "4",
                        "--num-tries", "2000", "--use-syntax-sequence",
                        "--num-threads", "1", "--random-string-generation", "ALPHANUMERIC",
                        "--host", "172.29.185.200", "--port", "6667", "--username", "root", "--password", "root",
                        "--database-prefix", "root.", "--max-generated-databases", "1",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.IOTDB_DATABASE_NAME, "--oracle", "TSAF"}));
    }

    @Test
    public void testTSAFWithoutS() {
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--log-syntax-error-query", "true", "--drop-database", "--max-expression-depth", "4",
                        "--num-tries", "2000", "--use-syntax-validator",
                        "--num-threads", "1", "--random-string-generation", "ALPHANUMERIC",
                        "--host", "172.29.185.200", "--port", "6667", "--username", "root", "--password", "root",
                        "--database-prefix", "root.", "--max-generated-databases", "1",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.IOTDB_DATABASE_NAME, "--oracle", "TSAF"}));
    }

    @Test
    public void testTSAFWithoutAll() {
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "-1", "--timeout-seconds", TestConfig.SECONDS,
                        "--log-syntax-error-query", "true", "--drop-database", "--max-expression-depth", "4",
                        "--num-tries", "2000",
                        "--num-threads", "1", "--random-string-generation", "ALPHANUMERIC",
                        "--host", "172.29.185.200", "--port", "6667", "--username", "root", "--password", "root",
                        "--database-prefix", "root.", "--max-generated-databases", "1",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.IOTDB_DATABASE_NAME, "--oracle", "TSAF"}));
    }

}
