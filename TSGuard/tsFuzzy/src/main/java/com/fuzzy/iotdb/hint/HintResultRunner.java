package com.fuzzy.iotdb.hint;

import com.benchmark.commonClass.TSFuzzyStatement;
import com.benchmark.entity.DBValResultSet;
import com.fuzzy.iotdb.IotDBGlobalState;
import com.fuzzy.iotdb.resultSet.IotDBResultSet;

public final class HintResultRunner {
    private HintResultRunner() {}

    /** Execute one SELECT and return a normalized snapshot, with logging. */
    public static IotDBResultNormalizer.Normalized runAndNormalize(IotDBGlobalState gs, String sql) throws Exception {
        // Ensure the SELECT appears in your .log files:
        gs.getLogger().writeCurrent(sql + ";");

        try (TSFuzzyStatement s = gs.getConnection().createStatement();
             DBValResultSet raw = s.executeQuery(sql)) {

            if (raw == null) {
                return new IotDBResultNormalizer.Normalized(
                        java.util.Collections.emptyList(),
                        new java.util.HashMap<>()
                );
            }
            // We expect IoTDB here:
            IotDBResultSet rs = (IotDBResultSet) raw;

            // IMPORTANT: do not iterate rs here and then normalize again;
            // DBValResultSet.resetCursor() is a no-op, so consume it once.
            return IotDBResultNormalizer.normalize(rs);
        }
    }
}
