package com.tsFuzzy.tsdbms.prometheus;

import com.fuzzy.Main;
import com.fuzzy.common.constant.GlobalConstant;
import com.tsFuzzy.tsdbms.TestConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestPrometheusPQS {

    @Test
    public void testPQS() {
        // assumeTrue(mysqlIsAvailable);
        assertEquals(0,
                Main.executeMain(new String[]{"--random-seed", "0", "--timeout-seconds", TestConfig.SECONDS,
                        "--num-threads", "1", "--random-string-generation", "ALPHANUMERIC",
                        "--host", "111.229.183.22", "--port", "9990",
                        "--database-prefix", "pqsdb", "--max-generated-databases", "1",
                        "--num-queries", TestConfig.NUM_QUERIES, GlobalConstant.IOTDB_DATABASE_NAME, "--oracle", "PQS"}));
    }

}
