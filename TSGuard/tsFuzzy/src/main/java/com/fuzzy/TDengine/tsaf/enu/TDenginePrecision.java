package com.fuzzy.TDengine.tsaf.enu;

import com.fuzzy.Randomly;

public enum TDenginePrecision {

    ms, us, ns;

    public static TDenginePrecision getRandom() {
        return Randomly.fromOptions(values());
    }

    public static boolean isExist(String precision) {
        try {
            TDenginePrecision.valueOf(precision);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
