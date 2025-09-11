package com.fuzzy.iotdb.hint;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class IotDBHintBuilder {
    private IotDBHintBuilder() {}

    // case-insensitive WHERE
    private static final Pattern P_HAS_WHERE = Pattern.compile("(?is)\\bWHERE\\b");
    private static final Pattern P_AFTER_WHERE = Pattern.compile("(?is)\\bWHERE\\b\\s*(.*)");
    private static final Pattern P_FOLLOWING_CLAUSES = Pattern.compile(
            "(?is)\\bGROUP\\s+BY\\b|\\bORDER\\s+BY\\b|\\bSLIMIT\\b|\\bLIMIT\\b|\\bOFFSET\\b|\\bALIGN\\s+BY\\s+DEVICE\\b|\\bFILL\\b|\\bHAVING\\b"
    );

    /** Build multiple semantics-preserving variants that parse reliably in IoTDB (no trailing comments). */
    public static List<String> safeVariants(String sql) {
        if (sql == null) return Collections.emptyList();
        String base = sql.trim();
        List<String> out = new ArrayList<>();

        if (P_HAS_WHERE.matcher(base).find()) {
            //WHERE TRUE AND (<body>)
            out.add(injectWhereTautology(base));
            // WHERE (<body>)
            out.add(parenthesizeWhere(base));

            WhereParts p = parseWhereParts(base);
            if (p != null) {
                // WHERE (1 = 1) AND (body)
                out.add(p.before + "WHERE (1 = 1) AND (" + p.body + ")" + p.suffix);
                // WHERE (time = time) AND (body)
                out.add(p.before + "WHERE (time = time) AND (" + p.body + ")" + p.suffix);
                // duplicate predicate
                out.add(p.before + "WHERE (" + p.body + ") AND (" + p.body + ") " + p.suffix);
                // deeper parentheses
                out.add(p.before + "WHERE ((" + p.body + ")) " + p.suffix);
                out.add(p.before + "WHERE (((" + p.body + "))) " + p.suffix);
                // OR FALSE
                out.add(p.before + "WHERE (" + p.body + ") OR (1=0) " + p.suffix);
                // identity  WHERE body
                //String bodyPlusZero = addPlusZeroToNumericLiterals(p.body);
                //out.add(p.before + "WHERE (" + bodyPlusZero + ") " + p.suffix);
            }
        } else {
            out.add(insertWhereTrue(base));
            out.add(insertWhereExpr(base, "(1 = 1)"));
            out.add(insertWhereExpr(base, "(time = time)"));
            //out.add(insertWhereExpr(base, "NOT (NOT TRUE)"));
        }

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

    /** Insert a WHERE <expr> in a valid place. */
    private static String insertWhereExpr(String sql, String expr) {
        Matcher m = P_FOLLOWING_CLAUSES.matcher(sql);
        if (m.find()) {
            int idx = m.start();
            return sql.substring(0, idx) + " WHERE " + expr + " " + sql.substring(idx);
        } else {
            return sql + " WHERE " + expr;
        }
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

    /** Safely split the query into: before 'WHERE', the WHERE body, and trailing suffix (GROUP BY / ORDER BY / ...). */
    private static WhereParts parseWhereParts(String sql) {
        Matcher afterWhere = P_AFTER_WHERE.matcher(sql);
        if (!afterWhere.find()) return null;

        int whereStart = afterWhere.start(); // index at 'WHERE'
        String beforeWhere = sql.substring(0, whereStart);

        String after = sql.substring(whereStart); // starts with WHERE ...
        // Find body vs. suffix
        Matcher follow = P_FOLLOWING_CLAUSES.matcher(after);
        if (follow.find()) {
            // "WHERE" + body + suffix
            int whereLen = matchWhereLen(after); // length of "WHERE" + whitespace
            String body = after.substring(whereLen, follow.start()).trim();
            String suffix = after.substring(follow.start());
            return new WhereParts(beforeWhere, body, " " + suffix);
        } else {
            int whereLen = matchWhereLen(after);
            String body = after.substring(whereLen).trim();
            return new WhereParts(beforeWhere, body, "");
        }
    }

    private static int matchWhereLen(String s) {
        Matcher m = Pattern.compile("(?is)^\\s*WHERE\\s+").matcher(s);
        if (m.find()) return m.end();
        // fallback: assume "WHERE " is present due to outer checks
        int idx = s.toLowerCase(Locale.ROOT).indexOf("where");
        if (idx >= 0) {
            int j = idx + "where".length();
            while (j < s.length() && Character.isWhitespace(s.charAt(j))) j++;
            return j;
        }
        return 0;
    }

    private static final class WhereParts {
        final String before; // text before the WHERE token
        final String body;   // WHERE predicate only
        final String suffix; // trailing clauses starting at GROUP BY/ORDER BY/...
        WhereParts(String before, String body, String suffix) {
            this.before = before;
            this.body = body;
            this.suffix = suffix;
        }
    }

//    private static String addPlusZeroToNumericLiterals(String body) {
//        // conservative: only plain decimal integers/floats
//        return body.replaceAll("(?<![\\w.])(\\d+(?:\\.\\d+)?)", "($1+0)");
//    }

}
