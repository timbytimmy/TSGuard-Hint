package com.fuzzy.iotdb.hint;

import lombok.var;

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
            out.add(injectWhereTautology(base)); //WHERE TRUE AND (<body>)
            out.add(parenthesizeWhere(base)); // WHERE (<body>)



            WhereParts p = parseWhereParts(base);
            if (p != null) {
                out.add(p.before + "WHERE (1 = 1) AND (" + p.body + ")" + p.suffix); // WHERE (1 = 1) AND (body)
                out.add(p.before + "WHERE (time = time) AND (" + p.body + ")" + p.suffix); // WHERE (time = time) AND (body)
                out.add(p.before + "WHERE (" + p.body + ") AND (" + p.body + ") " + p.suffix); // duplicate predicate
                out.add(p.before + "WHERE ((" + p.body + ")) " + p.suffix); // deeper parentheses
                out.add(p.before + "WHERE (((" + p.body + "))) " + p.suffix);
                out.add(p.before + "WHERE (" + p.body + ") OR (1=0) " + p.suffix); // OR FALSE
                String bodyPlusZero = addPlusZeroToNumericLiterals(p.body); //identity  WHERE body
                //out.add(p.before + "WHERE (" + bodyPlusZero + ") " + p.suffix); //already found bug
                out.add(andTrueAtEnd(p.before, p.body, p.suffix));
                String swapped = swapTopLevelAndVariant(base);
                if (swapped != null && !swapped.equals(base)) out.add(swapped); // swap value
                String flipped = flipEqIfSafeVariant(base);
                if (flipped != null && !flipped.equals(base)) out.add(flipped); //flip value
                out.add(p.before + "WHERE (" + p.body + ") OR FALSE" + p.suffix);
                out.add(notNot(p.before + "WHERE", p.body, p.suffix)); // NOT (NOT)
                out.add(timeLeNowAnd(p.before + "WHERE ", p.body, p.suffix)); //time before now
            }
        } else {
            out.add(insertWhereTrue(base));
            out.add(insertWhereExpr(base, "(1 = 1)"));
            out.add(insertWhereExpr(base, "(time = time)"));
            out.add(insertWhereExpr(base, "NOT (NOT TRUE)"));
            out.add(addOffsetZeroIfAbsent(base));
            out.add(addHugeLimitIfAbsent(base));
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

    // WHERE (<body>) AND TRUE
    private static String andTrueAtEnd(String beforeWhere, String body, String suffix) {
        return beforeWhere + "WHERE (" + body + ") AND TRUE" + suffix;
    }

    // Add OFFSET 0 if there's no OFFSET (harmless; stresses planner tail)
    private static String addOffsetZeroIfAbsent(String sql) {
        if (sql.matches("(?is).*\\bOFFSET\\b.*")) return sql;
        return sql + " OFFSET 0";
    }

    // Add a huge LIMIT if there is none (no-op cap)
    private static String addHugeLimitIfAbsent(String sql) {
        if (sql.matches("(?is).*\\bLIMIT\\b.*")) return sql;
        return sql + " LIMIT 9223372036854775807";
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

    private static String addPlusZeroToNumericLiterals(String body) {
        // conservative: only plain decimal integers/floats
        return body.replaceAll("(?<![\\w.])(\\d+(?:\\.\\d+)?)", "($1+0)");
    }

    private static String notNot(String beforeWhere, String body, String after) {
        return beforeWhere + " NOT (NOT (" + body + "))" + after;
    }

    private static String orFalse(String beforeWhere, String body, String after) {
        return beforeWhere + " (" + body + ") OR FALSE" + after;
    }

    private static String timeLeNowAnd(String beforeWhere, String body, String after) {
        return beforeWhere + " (time <= now()) AND (" + body + ")" + after;
    }

    //swap value and flip
    private static List<String> splitTopLevelAnds(String body) {
        List<String> parts = new ArrayList<>();
        int depth = 0, last = 0;
        for (int i = 0; i < body.length(); i++) {
            char c = body.charAt(i);
            if (c == '(') { depth++; continue; }
            if (c == ')') { if (depth > 0) depth--; continue; }

            if (depth == 0) {
                // case-insensitive AND with word boundaries
                if (i + 3 <= body.length()
                        && (body.regionMatches(true, i, "AND", 0, 3))
                        && isWordBoundary(body, i - 1)
                        && isWordBoundary(body, i + 3)) {
                    parts.add(body.substring(last, i).trim());
                    i += 2; // skip 'ND' (for-loop ++ will advance past 'D')
                    last = i + 1;
                }
            }
        }
        parts.add(body.substring(last).trim());
        return parts;
    }

    private static boolean isWordBoundary(String s, int idx) {
        if (idx < 0 || idx >= s.length()) return true;
        char ch = s.charAt(idx);
        return !(Character.isLetterOrDigit(ch) || ch == '_');
    }

    private static String swapTopLevelAndVariant(String sql) {
        WhereParts p = parseWhereParts(sql);
        if (p == null) return null;

        List<String> ands = splitTopLevelAnds(p.body);
        if (ands.size() != 2) return null; // only swap exactly two conjuncts

        String a = ands.get(0);
        String b = ands.get(1);
        return p.before + "WHERE (" + b + ") AND (" + a + ")" + p.suffix;
    }

    private static String flipEqIfSafeVariant(String sql) {
        WhereParts p = parseWhereParts(sql);
        if (p == null) return null;

        List<String> ands = splitTopLevelAnds(p.body);
        boolean changed = false;
        List<String> newAnds = new ArrayList<>();

        for (String atom : ands) {
            String trimmed = stripOuterParens(atom);
            String flipped = flipLiteralEqColumn(trimmed);
            if (!flipped.equals(trimmed)) changed = true;
            // keep a single layer of parens around each atom for safety
            newAnds.add("(" + flipped + ")");
        }
        if (!changed) return null;

        String joined = String.join(" AND ", newAnds);
        return p.before + "WHERE " + joined + p.suffix;
    }

    private static String stripOuterParens(String s) {
        String t = s.trim();
        while (t.startsWith("(") && t.endsWith(")")) {
            int depth = 0; boolean ok = true;
            for (int i = 0; i < t.length(); i++) {
                char c = t.charAt(i);
                if (c == '(') depth++;
                else if (c == ')') { depth--; if (depth < 0) { ok = false; break; } }
                if (i < t.length() - 1 && depth == 0) { ok = false; break; }
            }
            if (!ok) break;
            t = t.substring(1, t.length() - 1).trim();
        }
        return t;
    }

    private static final Pattern P_LIT_EQ_COL = Pattern.compile(
            // literal (number or double-quoted string) on the left, column/path on the right
            "(?is)^\\s*(?:([-+]?\\d+(?:\\.\\d+)?)|(\"(?:[^\"\\\\]|\\\\.)*\"))\\s*=\\s*([A-Za-z_][A-Za-z0-9_\\.]*?)\\s*$"
    );

    private static final Pattern P_COL_EQ_LIT = Pattern.compile(
            // column/path on the left, literal on the right (already preferred)
            "(?is)^\\s*([A-Za-z_][A-Za-z0-9_\\.]*)\\s*=\\s*((?:[-+]?\\d+(?:\\.\\d+)?)|(?:\"(?:[^\"\\\\]|\\\\.)*\"))\\s*$"
    );

    /** If atom is 'literal = column', return 'column = literal'; otherwise return atom unchanged. */
    private static String flipLiteralEqColumn(String atom) {
        Matcher m1 = P_LIT_EQ_COL.matcher(atom);
        if (m1.matches()) {
            String lit = m1.group(1) != null ? m1.group(1) : m1.group(2); // number or quoted string
            String col = m1.group(3);
            return col + " = " + lit;
        }
        // If already 'col = lit', keep it
        Matcher m2 = P_COL_EQ_LIT.matcher(atom);
        if (m2.matches()) return atom;
        // Otherwise, leave atom untouched
        return atom;
    }








}
