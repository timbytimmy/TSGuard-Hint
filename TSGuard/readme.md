Minimum Requirements:

- Java 8 or above
- Maven
- Lombok
- InfluxDB: 2.7.11 or earlier
- IotDB: 1.3.3 or earlier
- TDengine: 3.3.4.8 or earlier





If you want to run the relevant tests, you need to:

1. Configure the Java environment.
2. Execute the test classes:
   - `com.tsFuzzy.tsdbms.influxdb.testTSAF`
   - `com.tsFuzzy.tsdbms.iotdb.testTSAF`
   - `com.tsFuzzy.tsdbms.tdengine.testTSAF`

**Prerequisites**:

- Ensure your database instance (e.g., InfluxDB, IoTDB, TDengine) is running.
- Configure required parameters such as `host`, `port`, `username`, and `password`.