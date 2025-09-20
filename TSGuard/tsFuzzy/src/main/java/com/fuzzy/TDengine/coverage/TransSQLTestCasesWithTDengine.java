package com.fuzzy.TDengine.coverage;

import com.fuzzy.common.coverage.TransSQLTestCasesService;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;

@Slf4j
public class TransSQLTestCasesWithTDengine implements TransSQLTestCasesService {
    private final static String createRegex = "CREATE";
    private final static String insertRegex = "INSERT INTO";
    private final static String selectRegex = "SELECT";
    private static long queryCount = 0L;
    private static StringBuilder sb = new StringBuilder();

    public static void setQueryCount(long queryCount) {
        TransSQLTestCasesWithTDengine.queryCount = queryCount;
    }

    @Override
    public String transSQLTestCase(String sqlFilePath) {
        sb.setLength(0);
        preInitTestCase();
        try {
            BufferedReader br = new BufferedReader(new FileReader(sqlFilePath));
            String line;
            while ((line = br.readLine()) != null) {
                String pyCode = convertSqlStatements(line);
                if (!pyCode.isEmpty())
                    sb.append(pyCode).append("\n");
            }
            br.close();
        } catch (Exception e) {
            log.error("转换SQL测试用例失败, sqlFilePath:{} e:", sqlFilePath, e);
        }
        endAppend();
        return sb.toString();
    }

    private void preInitTestCase() {
        sb.append("# -*- coding: utf-8 -*-\n" +
                "\n" +
                "import sys\n" +
                "import taos\n" +
                "from util.log import tdLog\n" +
                "from util.cases import tdCases\n" +
                "from util.sql import tdSql\n" +
                "from util.dnodes import tdDnodes\n\n");

        sb.append("class TDTestCase:\n" +
                        "    def init(self, conn, logSql, replicaVar = 1):\n" +
                        "        tdLog.debug(\"start to execute %s\" % __file__)\n" +
                        "        tdSql.init(conn.cursor(), logSql)\n" +
                        "\n" +
                        "        self.ts = ").append(System.currentTimeMillis())
                .append("\n\n");

        sb.append("    def run(self):\n" +
                "        tdSql.prepare()\n");
    }

    private static String convertSqlStatements(String sql) {
        sql = sql.replace("\"", "\\\"");
        if (sql.startsWith(createRegex) || sql.startsWith(insertRegex))
            return String.format("        tdSql.execute(\"%s\")", sql);
        else if (sql.startsWith(selectRegex)) {
            queryCount++;
            return String.format("        tdSql.query(\"%s\")", sql);
        }
        else {
            log.info("忽略该类型语句, sql:{}", sql);
            return "";
        }
    }

    private void endAppend() {
        sb.append("\n").append("    def stop(self):\n" +
                "        tdSql.close()\n" +
                "        tdLog.success(\"%s successfully executed\" % __file__)\n" +
                "\n" +
                "\n" +
                "tdCases.addWindows(__file__, TDTestCase())\n" +
                "tdCases.addLinux(__file__, TDTestCase())\n");
    }

    public static long getQueryCount() {
        return queryCount;
    }
}
