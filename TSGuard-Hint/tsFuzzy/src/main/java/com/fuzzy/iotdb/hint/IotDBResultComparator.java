package com.fuzzy.iotdb.hint;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class IotDBResultComparator {

    public static final class Diff {
        public final String message;
        public Diff(String message) { this.message = message; }
        @Override public String toString() { return message; }
    }

    public static Diff compare(IotDBResultNormalizer.Normalized a,
                               IotDBResultNormalizer.Normalized b) {
        // 1) Same schema (value columns only)
        if (!Objects.equals(a.valueColumnLabels, b.valueColumnLabels)) {
            return new Diff("Column labels differ: \nA=" + a.valueColumnLabels + "\nB=" + b.valueColumnLabels);
        }
        // 2) Same row multiset
        if (!a.multiset.equals(b.multiset)) {
            // find a small, helpful diff
            String firstOnlyInA = firstKeyOnlyInLeft(a.multiset, b.multiset);
            String firstOnlyInB = firstKeyOnlyInLeft(b.multiset, a.multiset);
            return new Diff("Row multiset differs.\n" +
                    (firstOnlyInA != null ? "Example only in baseline: " + firstOnlyInA + "\n" : "") +
                    (firstOnlyInB != null ? "Example only in hinted:  " + firstOnlyInB + "\n" : "") +
                    "Sizes: A=" + total(a.multiset) + " B=" + total(b.multiset));
        }
        return null; // equal
    }

    private static String firstKeyOnlyInLeft(Map<String,Integer> l, Map<String,Integer> r) {
        for (Map.Entry<String,Integer> e : l.entrySet()) {
            int lc = e.getValue() == null ? 0 : e.getValue();
            int rc = r.getOrDefault(e.getKey(), 0);
            if (lc != rc) return e.getKey() + " (countA=" + lc + ", countB=" + rc + ")";
        }
        return null;
    }

    private static int total(Map<String,Integer> m) {
        int s = 0;
        for (Integer v : m.values()) s += v == null ? 0 : v;
        return s;
    }
}
