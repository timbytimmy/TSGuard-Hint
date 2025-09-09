package com.fuzzy.iotdb.hint;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class IotDBHintBuilder {
    private IotDBHintBuilder() {}

    // case-insensitive WHERE
    private static final Pattern P_HAS_WHERE = Pattern.compile("(?is)\\bWHERE\\b");

    // capture everything after WHERE so we can wrap it: WHERE <rest>
    private static final Pattern P_AFTER_WHERE = Pattern.compile("(?is)\\bWHERE\\b\\s*(.*)");

    // tokens that (if present) must come AFTER WHERE in IoTDB grammar
    // we insert WHERE TRUE before the earliest of these when original SQL has no WHERE
    private static final Pattern P_FOLLOWING_CLAUSES = Pattern.compile(
            "(?is)\\bGROUP\\s+BY\\b|\\bORDER\\s+BY\\b|\\bSLIMIT\\b|\\bLIMIT\\b|\\bOFFSET\\b|\\bALIGN\\s+BY\\s+DEVICE\\b|\\bFILL\\b|\\bHAVING\\b"
    );

    /** Build multiple semantics-preserving variants that parse reliably in IoTDB (no trailing comments). */
    public static List<String> safeVariants(String sql) {
        if (sql == null) return Collections.emptyList();
        String base = sql.trim();
        List<String> out = new ArrayList<>();

        if (P_HAS_WHERE.matcher(base).find()) {
            // Variant A: WHERE TRUE AND (<original-where-body>)
            out.add(injectWhereTautology(base));

            // Variant B: just add redundant parentheses: WHERE (<original-where-body>)
            out.add(parenthesizeWhere(base));
        } else {
            // Variant C: insert WHERE TRUE at the correct spot (before GROUP BY / ORDER BY / SLIMIT / ...)
            out.add(insertWhereTrue(base));
        }

        // de-dup, keep order
        LinkedHashSet<String> set = new LinkedHashSet<>(out);
        return new ArrayList<>(set);
    }

    /** Replace the WHERE body with: TRUE AND (<body>) */
    private static String injectWhereTautology(String sql) {
        Matcher m = P_AFTER_WHERE.matcher(sql);
        if (m.find()) {
            String body = m.group(1);
            return m.replaceFirst("WHERE TRUE AND (" + body + ")");
        }
        // fallback (shouldn’t happen if we detected WHERE)
        return sql.replaceFirst("(?i)\\bWHERE\\b", "WHERE TRUE AND");
    }

    /** Wrap the WHERE body in parentheses: WHERE (<body>) */
    private static String parenthesizeWhere(String sql) {
        Matcher m = P_AFTER_WHERE.matcher(sql);
        if (m.find()) {
            String body = m.group(1);
            return m.replaceFirst("WHERE (" + body + ")");
        }
        return sql; // nothing to do
    }

    /** Insert WHERE TRUE in a valid place: before GROUP BY / ORDER BY / SLIMIT / LIMIT / OFFSET / ALIGN BY DEVICE / FILL / HAVING; otherwise at end. */
    private static String insertWhereTrue(String sql) {
        Matcher m = P_FOLLOWING_CLAUSES.matcher(sql);
        if (m.find()) {
            int idx = m.start();
            return sql.substring(0, idx) + " WHERE TRUE " + sql.substring(idx);
        } else {
            return sql + " WHERE TRUE";
        }
    }
}
