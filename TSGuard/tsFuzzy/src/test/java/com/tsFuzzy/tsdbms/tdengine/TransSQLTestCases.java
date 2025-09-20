package com.tsFuzzy.tsdbms.tdengine;

import com.fuzzy.TDengine.coverage.TransSQLTestCasesWithTDengine;
import com.fuzzy.common.coverage.TransSQLTestCaseUtils;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class TransSQLTestCases {
    private static final String sourceSQLFilePath = "E:\\work project\\t3-tsms\\tsFuzzy\\logs\\10\\tdengine\\pqs";
    private static final String targetSQLFilePath = "E:\\work project\\t3-tsms\\tsFuzzy\\logs\\10\\tdengine\\pqs\\py";

    @Test
    public void getSamplingFrequency() throws Exception {
        TransSQLTestCaseUtils.transSQLTestCaseInDirForTDengine(sourceSQLFilePath, targetSQLFilePath);
        log.info("queryCount: {}", TransSQLTestCasesWithTDengine.getQueryCount());
    }

    @Test
    public void renameFile() throws Exception {
        File directory = new File(sourceSQLFilePath);
        if (directory.exists() && directory.isDirectory()) {
            File[] files = directory.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isFile()) {
                        String fileName = file.getName();
                        Pattern pattern = Pattern.compile("(\\d+)");
                        Matcher matcher = pattern.matcher(fileName);

                        if (matcher.find()) {
                            String numberStr = matcher.group(1);
                            int number = Integer.parseInt(numberStr);
                            int newNumber = number + 150;
                            String newFileName = fileName.replace(numberStr, String.valueOf(newNumber));
                            File newFile = new File(directory, newFileName);
                            boolean success = file.renameTo(newFile);
                            if (success) {
                                System.out.println("文件重命名成功: " + fileName + " -> " + newFileName);
                            } else {
                                System.out.println("文件重命名失败: " + fileName);
                            }
                        }
                    }
                }
            } else {
                System.out.println("该目录下没有文件.");
            }
        } else {
            System.out.println("指定路径不是一个有效的目录.");
        }
    }
}
