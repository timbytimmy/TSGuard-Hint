package com.fuzzy.influxdb.util;

import lombok.Data;

@Data
public class InfluxDBAuthorizationParams {
    private String organizationId;
    private String token;
}
