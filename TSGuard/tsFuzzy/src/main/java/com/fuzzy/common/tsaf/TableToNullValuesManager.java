package com.fuzzy.common.tsaf;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TableToNullValuesManager {
    // databaseName_tableName -> null values
    private static final ConcurrentHashMap<String, Set<Long>> tableToNullValuesMap = new ConcurrentHashMap<>();

    public static void addNullValueToTable(String databaseName, String tableName, Long timestamp) {
        String hashKey = hashKey(databaseName, tableName);
        if (!tableToNullValuesMap.containsKey(hashKey)) {
            tableToNullValuesMap.put(hashKey, new HashSet<>(Collections.singletonList(timestamp)));
        } else {
            tableToNullValuesMap.get(hashKey).add(timestamp);
        }
    }

    public static Set<Long> getNullValues(String databaseName, String tableName) {
        return tableToNullValuesMap.getOrDefault(hashKey(databaseName, tableName), new HashSet<>());
    }

    private static String hashKey(String databaseName, String tableName) {
        return databaseName + "_" + tableName;
    }
}
