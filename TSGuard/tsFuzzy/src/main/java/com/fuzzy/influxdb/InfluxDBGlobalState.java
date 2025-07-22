    package com.fuzzy.influxdb;

    import com.fuzzy.SQLGlobalState;
    import com.fuzzy.influxdb.InfluxDBOptions.InfluxDBOracleFactory;

    public class InfluxDBGlobalState extends SQLGlobalState<InfluxDBOptions, InfluxDBSchema> {

        //setting time range
        // Track the actual timestamp range of inserted data
        private long insertedMinTimestamp = Long.MAX_VALUE;
        private long insertedMaxTimestamp = Long.MIN_VALUE;

        /**
         * Update the recorded min/max timestamps whenever a new point is written.
         */
        public void updateInsertedTimestamps(long ts) {
            if (ts < insertedMinTimestamp) {
                insertedMinTimestamp = ts;
            }
            if (ts > insertedMaxTimestamp) {
                insertedMaxTimestamp = ts;
            }
        }

        public long getInsertedMinTimestamp() {
            return insertedMinTimestamp;
        }

        public long getInsertedMaxTimestamp() {
            return insertedMaxTimestamp;
        }

        //original one, no touch

        @Override
        protected InfluxDBSchema readSchema() throws Exception {
            return InfluxDBSchema.fromConnection(getConnection(), getDatabaseName());
        }

        public boolean usesPQS() {
            return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == InfluxDBOracleFactory.PQS);
        }

        public boolean usesTSAF() {
            return getDbmsSpecificOptions().oracles.stream().anyMatch(o -> o == InfluxDBOptions.InfluxDBOracleFactory.TSAF);
        }

        public boolean usesHINT(){
            return getDbmsSpecificOptions().oracles.stream().anyMatch((o -> o == InfluxDBOptions.InfluxDBOracleFactory.HINT));
        }
    }
