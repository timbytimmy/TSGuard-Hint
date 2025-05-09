package com.tsFuzzy.tsdbms.griddb;

import com.fuzzy.Main;
import com.fuzzy.common.constant.GlobalConstant;
import com.tsFuzzy.tsdbms.TestConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@Slf4j
public class TestGridDB {

    @Test
    public void testPQS() {
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "1", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--max-expression-depth", "5", "--drop-database",
                        "--num-tries", "500", "--use-syntax-sequence", "--use-syntax-validator",
                        "--random-string-generation", "ALPHANUMERIC", "--log-syntax-error-query", "true",
                        "--host", "127.0.0.1", "--port", "20001", "--username", "admin", "--password", "admin",
                        "--database-prefix", "pqsdb", "--max-generated-databases", "1", "--start-timestamp", "1641024000",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.GRIDDB_DATABASE_NAME, "--oracle", "PQS"}));
    }

    @Test
    public void testTSAF() {
        // "--drop-database",
        // "--use-syntax-sequence"
        // "--use-syntax-validator"
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "23", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--max-expression-depth", "5",
                        "--num-tries", "500", "--use-syntax-sequence", "--use-syntax-validator",
                        "--random-string-generation", "ALPHANUMERIC", "--log-syntax-error-query", "true",
                        "--host", "127.0.0.1", "--port", "20001", "--username", "admin", "--password", "admin",
                        "--database-prefix", "tsafdb", "--max-generated-databases", "1", "--start-timestamp", "1641024000",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.GRIDDB_DATABASE_NAME, "--oracle", "TSAF"}));
    }

}
