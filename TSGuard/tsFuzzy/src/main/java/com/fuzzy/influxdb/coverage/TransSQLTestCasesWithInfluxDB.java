package com.fuzzy.influxdb.coverage;

import com.fuzzy.common.coverage.TransSQLTestCasesService;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class TransSQLTestCasesWithInfluxDB implements TransSQLTestCasesService {
    private final static String createRegex = "t";
    private final static String insertRegex = "t";
    private final static String selectRegex = "q=SELECT";
    private final static String influxDBHeaderFilePath = "src/main/java/com/fuzzy/common/coverage/resources/query.rs";

    private static StringBuilder sb = new StringBuilder();
    private boolean isFirstTransQuerySql = true;

    static {
        preInitTestCase();
    }

    @Override
    public String transSQLTestCase(String sqlFilePath) {
        isFirstTransQuerySql = true;
        String fileName = Paths.get(sqlFilePath).getFileName().toString();
        String databaseName = fileName.split("-")[0];

        appendHeader(fileName.split("\\.")[0].replace("-", "_"));
        try {
            BufferedReader br = new BufferedReader(new FileReader(sqlFilePath));
            String line;
            while ((line = br.readLine()) != null) {
                String rustCode = convertSqlStatements(line, databaseName);
                if (!rustCode.isEmpty())
                    sb.append(rustCode).append("\n");
            }
            br.close();
        } catch (Exception e) {
            log.error("转换SQL测试用例失败, sqlFilePath:{} e:", sqlFilePath, e);
        }

        appendFooter();
        return sb.toString();
    }

    private String convertSqlStatements(String sql, String databaseName) {
        sql = sql.replace("\"", "\\\"").replace(";", "");
        sql = processTimestamps(sql);
        StringBuilder sqlBuilder = new StringBuilder();
        if (sql.startsWith(createRegex) || sql.startsWith(insertRegex))
            sqlBuilder.append("    server").append("\n")
                    .append("        .write_lp_to_db(").append("\n")
                    .append(String.format("            \"%s\",", databaseName)).append("\n")
                    .append(String.format("            \"%s\",", sql.replace("000000000", "")
                            .replace("\\n", "\\n\\\n             ")))
                    .append("\n")
                    .append("            Precision::Second,").append("\n")
                    .append("        )\n" +
                            "        .await\n" +
                            "        .unwrap();\n");
        else if (sql.startsWith(selectRegex)) {
            if (isFirstTransQuerySql) {
                sqlBuilder.append("    let test_cases = [").append("\n");
                isFirstTransQuerySql = false;
            }

            sqlBuilder.append(String.format("        TestCase {\n" +
                    "            database: Some(\"%s\"),\n" +
                    "            query: \"%s\",\n" +
                    "            expected: \"\",\n" +
                    "        },", databaseName, sql.replace("q=", "")));
        }
        else {
            log.info("忽略该类型语句, sql:{}", sql);
            return "";
        }
        return sqlBuilder.toString();
    }

    // influxdb 测试时间戳不能低于当前时间3days，因此将所有时间戳全部加上指定值，超过当前日期即可
    private String processTimestamps(String sql) {
        String regex = "\\b\\d{19}\\b";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(sql);

        StringBuffer modifiedString = new StringBuffer();
        while (matcher.find()) {
            String timestampStr = matcher.group();
            long timestamp = Long.parseLong(timestampStr);
            long newTimestamp = timestamp + 1000000000000000000L;
            matcher.appendReplacement(modifiedString, String.valueOf(newTimestamp));
        }
        matcher.appendTail(modifiedString);
        return modifiedString.toString();
    }

    private static void preInitTestCase() {
        try {
            BufferedReader br = new BufferedReader(new FileReader(influxDBHeaderFilePath));
            String line;
            while ((line = br.readLine()) != null) {
                sb.append(line).append("\n");
            }
            br.close();
        } catch (Exception e) {
            log.error("读取influxdb query异常, query.rs:{} e:", influxDBHeaderFilePath, e);
        }
    }

    private void appendHeader(String fileName) {
        // 添加函数附加头
        sb.append("\n#[tokio::test]\n")
                .append(String.format("async fn api_v3_query_sql_tsaf_%s() {\n", fileName))
                .append("    let server = TestServer::spawn().await;\n\n");
    }

    private void appendFooter() {
        // 添加函数尾
        sb.append("    ];\n" +
                "\n" +
                "    for t in test_cases {\n" +
                "        let mut params = vec![(\"q\", t.query), (\"format\", \"pretty\")];\n" +
                "        if let Some(db) = t.database {\n" +
                "            params.push((\"db\", db))\n" +
                "        }\n" +
                "        let resp = server.api_v3_query_sql(&params).await.text().await.unwrap();\n" +
                "        println!(\"\\n{q}\", q = t.query);\n" +
                "        println!(\"{resp}\");\n" +
                "    }\n" +
                "}");
    }
}
