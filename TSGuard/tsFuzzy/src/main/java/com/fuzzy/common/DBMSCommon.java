package com.fuzzy.common;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DBMSCommon {

    private static final Pattern TSGUARD_INDEX_PATTERN = Pattern.compile("^i\\d+");

    private DBMSCommon() {
    }

    public static String createTableName(int nr) {
        return String.format("t%d", nr);
    }

    public static String createColumnName(int nr, boolean isTag, String tableName) {
        // TODO 等待回复, 如何查询多个时序数据相同字段键？
        if (isTag) return String.format("%s_tag%d", tableName, nr);
        else return String.format("%s_f%d", tableName, nr);
    }

    public static String createColumnName(int nr, String databaseName, String tableName) {
        return String.format("%s_%s_c%d", databaseName, tableName, nr);
    }

    public static String createColumnName(int nr) {
        return String.format("c%d", nr);
    }

    public static String createIndexName(int nr) {
        return String.format("i%d", nr);
    }

    public static boolean matchesIndexName(String indexName) {
        Matcher matcher = TSGUARD_INDEX_PATTERN.matcher(indexName);
        return matcher.matches();
    }

    public static int getMaxIndexInDoubleArray(double... doubleArray) {
        int maxIndex = 0;
        double maxValue = 0.0;
        for (int j = 0; j < doubleArray.length; j++) {
            double curReward = doubleArray[j];
            if (curReward > maxValue) {
                maxIndex = j;
                maxValue = curReward;
            }
        }
        return maxIndex;
    }

    public static boolean areQueryPlanSequencesSimilar(List<String> list1, List<String> list2) {
        return editDistance(list1, list2) <= 1;
    }

    public static int editDistance(List<String> list1, List<String> list2) {
        int[][] dp = new int[list1.size() + 1][list2.size() + 1];
        for (int i = 0; i <= list1.size(); i++) {
            for (int j = 0; j <= list2.size(); j++) {
                if (i == 0) {
                    dp[i][j] = j;
                } else if (j == 0) {
                    dp[i][j] = i;
                } else {
                    dp[i][j] = Math.min(dp[i - 1][j - 1] + costOfSubstitution(list1.get(i - 1), list2.get(j - 1)),
                            Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1));
                }
            }
        }
        return dp[list1.size()][list2.size()];
    }

    private static int costOfSubstitution(String string, String string2) {
        return string.equals(string2) ? 0 : 1;
    }

}
