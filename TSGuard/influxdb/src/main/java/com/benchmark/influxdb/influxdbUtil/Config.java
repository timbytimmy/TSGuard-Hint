package com.benchmark.influxdb.influxdbUtil;

import com.benchmark.influxdb.Globals;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

public class Config {
    private static Config config = new Config();
    private ResourceBundle resource;

    public Config() {
        try {
            InputStream inputStream = new BufferedInputStream(
                    new FileInputStream(new File("./conf/influxdb/sys.properties")));
            resource = new PropertyResourceBundle(inputStream);
            String host = resource.getString("host");
            Globals.HOST = host;

        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
    }

    public String getDataPathRoot() {
        return this.resource.getString("dataPathRoot");
    }

    public String getDbHost() {
        return this.resource.getString("dbHost");
    }

    public String getPointName() {
        return this.resource.getString("pointName");
    }

    public String getBegin() {
        return this.resource.getString("begin");
    }

    public String getEnd() {
        return this.resource.getString("end");
    }

    public long getAid() {
        return Long.parseLong(this.resource.getString("aid"));
    }

    public int getK() {
        return Integer.parseInt(this.resource.getString("k"));
    }

    public int getLookback() {
        return Integer.parseInt(this.resource.getString("lookback"));
    }

    public static Config getInstance() {
        return config;
    }

    public static void init() {
    }


}
