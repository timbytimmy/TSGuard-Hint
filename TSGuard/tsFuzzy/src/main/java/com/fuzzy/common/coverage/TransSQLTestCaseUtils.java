package com.fuzzy.common.coverage;

import com.fuzzy.TDengine.coverage.TransSQLTestCasesWithTDengine;
import com.fuzzy.influxdb.coverage.TransSQLTestCasesWithInfluxDB;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
public class TransSQLTestCaseUtils {
    private static final String sourceFileSuffix = "-cur.log";

    public static void transSQLTestCaseInDirForTDengine(String sourceDir, String targetDir) {
        try {
            TransSQLTestCasesService transSQLTestCasesService = new TransSQLTestCasesWithTDengine();
            List<String> filePaths = FileUtils.getFilesWithSuffix(sourceDir, sourceFileSuffix);
            for (int i = 0; i < filePaths.size(); i++) {
                String filePath = filePaths.get(i);
                String targetFilePath = Paths.get(filePath).getFileName().toString().split("\\.")[0];

                // trans
                String pyCode = transSQLTestCasesService.transSQLTestCase(filePath);
                FileUtils.writeToFile(pyCode, Paths.get(targetDir, targetFilePath + ".py").toString());
            }
        } catch (IOException e) {
            log.error("transSQLTestCase 文件转换IO异常, e:", e);
        } catch (Exception e) {
            log.error("transSQLTestCase 转换数据异常, e:", e);
        }
    }

    public static void transSQLTestCaseInDirForInfluxDB(String sourceDir, String targetFilePath) {
        try {
            StringBuffer sb = new StringBuffer();
            TransSQLTestCasesService transSQLTestCasesService = new TransSQLTestCasesWithInfluxDB();
            List<String> filePaths = FileUtils.getFilesWithSuffix(sourceDir, sourceFileSuffix);
            for (int i = 0; i < filePaths.size(); i++) {
                String filePath = filePaths.get(i);

                // trans
                sb.append(transSQLTestCasesService.transSQLTestCase(filePath));
                if (i == filePaths.size() - 1)
                    FileUtils.writeToFile(sb.toString(), targetFilePath);
            }
        } catch (IOException e) {
            log.error("transSQLTestCase 文件转换IO异常, e:", e);
        } catch (Exception e) {
            log.error("transSQLTestCase 转换数据异常, e:", e);
        }
    }
}
