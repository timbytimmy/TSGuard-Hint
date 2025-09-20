package com.fuzzy.iotdb.hint;

import com.fuzzy.iotdb.IotDBSchema;
import com.fuzzy.iotdb.resultSet.IotDBResultSet;

import java.math.BigDecimal;
import java.math.MathContext;
import java.sql.SQLException;
import java.util.*;

/** Normalizes IoTDB results so we can compare baseline vs hinted order-insensitively. */
public final class IotDBResultNormalizer {

    public static final class Normalized {
        public final List<String> valueColumnLabels; // without TIME
        public final Map<String, Integer> multiset;  // rowKey -> count

        Normalized(List<String> labels, Map<String, Integer> multiset) {
            this.valueColumnLabels = labels;
            this.multiset = multiset;
        }
    }

    // Quantize numeric values so tiny FP noise doesn't trigger diffs.
    private static final MathContext MC = new MathContext(12); // ~1e-12 precision

    public static Normalized normalize(IotDBResultSet rs) throws SQLException {
        // Column names that IoTDB returns usually include TIME as the first; values list excludes TIME.
        List<String> allCols = rs.getColumnNames();        // includes TIME
        if (allCols == null || allCols.isEmpty()) {
            return new Normalized(Collections.emptyList(), new HashMap<>());
        }

        // Build the list of value columns (skip TIME)
        List<String> valueCols = new ArrayList<>();
        for (String c : allCols) {
            if (!"time".equalsIgnoreCase(c)) {
                valueCols.add(c);
            }
        }

        Map<String, Integer> multiset = new HashMap<>();

        while (rs.hasNext()) {
            // The IotDBResultSet moves the cursor in hasNext(); get current row values now.
            long ts = rs.getTimestamp().getTime(); // millis
            List<String> vals = new ArrayList<>(valueCols.size());

            for (int idx = 0; idx < valueCols.size(); idx++) {
                if (rs.valueIsNull(idx)) {
                    vals.add("NULL");
                } else {
                    IotDBSchema.IotDBDataType dt = rs.getColumnType(valueCols.get(idx));
                    switch (dt) {
                        case DOUBLE:
                        case FLOAT: {
                            double v = rs.getDouble(idx);
                            vals.add(canonNumber(v));
                            break;
                        }
                        case INT32: {
                            int v = rs.getInt(idx);
                            vals.add(Long.toString(v));
                            break;
                        }
                        case INT64: {
                            long v = rs.getLong(idx);
                            vals.add(Long.toString(v));
                            break;
                        }
                        case BIGDECIMAL: {
                            BigDecimal v = new BigDecimal(rs.getDouble(idx), MC);
                            vals.add(canonBigDecimal(v));
                            break;
                        }
                        case BOOLEAN: {
                            // store lowercase for canonical form
                            vals.add(rs.getString(idx) == null ? "NULL" : rs.getString(idx).toLowerCase(Locale.ROOT));
                            break;
                        }
                        case TEXT:
                        case NULL:
                        default: {
                            String v = rs.getString(idx);
                            vals.add(v == null ? "NULL" : v);
                            break;
                        }
                    }
                }
            }

            String rowKey = buildRowKey(ts, vals);
            multiset.merge(rowKey, 1, Integer::sum);
        }

        return new Normalized(valueCols, multiset);
    }

    private static String buildRowKey(long ts, List<String> vals) {
        // Canonical, order-insensitive compare: (timestamp|v1|v2|...|vN)
        StringBuilder sb = new StringBuilder();
        sb.append(ts).append('|');
        for (int i = 0; i < vals.size(); i++) {
            if (i > 0) sb.append('|');
            sb.append(vals.get(i) == null ? "NULL" : vals.get(i));
        }
        return sb.toString();
    }

    private static String canonNumber(double v) {
        // Quantize doubles using BigDecimal with a math context
        return new BigDecimal(v, MC).toPlainString();
    }

    private static String canonBigDecimal(BigDecimal v) {
        return v.round(MC).toPlainString();
    }

    public static Normalized empty() {
        return new Normalized(java.util.Collections.emptyList(), new java.util.HashMap<>());
    }
}
