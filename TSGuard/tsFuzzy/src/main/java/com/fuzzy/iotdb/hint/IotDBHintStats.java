// com.fuzzy.iotdb.hint.IotDBHintStats
package com.fuzzy.iotdb.hint;

import java.util.concurrent.atomic.AtomicInteger;

public final class IotDBHintStats {
    public static final AtomicInteger PAIRS = new AtomicInteger(0);
    public static final AtomicInteger MATCHES = new AtomicInteger(0);
    public static final AtomicInteger MISMATCHES = new AtomicInteger(0);
    public static final java.util.concurrent.atomic.AtomicInteger ENGINE_ERRORS = new java.util.concurrent.atomic.AtomicInteger();

    private IotDBHintStats() {}

}
