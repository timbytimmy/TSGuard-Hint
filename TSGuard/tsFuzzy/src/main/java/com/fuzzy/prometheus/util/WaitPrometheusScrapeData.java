package com.fuzzy.prometheus.util;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WaitPrometheusScrapeData {
    private static long waitTime = 1000;

    public static void waitPrometheusScrapeData() {
        try {
            Thread.sleep(waitTime);
        } catch (Exception e) {
            log.error("休眠异常:", e);
        }
    }
}
