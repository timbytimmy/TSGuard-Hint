    package com.fuzzy;

    import com.benchmark.entity.DBValResultSet;
    import com.beust.jcommander.JCommander;
    import com.beust.jcommander.JCommander.Builder;
    import com.fuzzy.TDengine.feedback.TDengineQuerySynthesisFeedbackManager;
    import com.fuzzy.common.constant.GlobalConstant;
    import com.fuzzy.common.log.Loggable;
    import com.fuzzy.common.query.Query;
    import com.fuzzy.common.tsaf.ConstraintValue;
    import com.fuzzy.influxdb.feedback.InfluxDBQuerySynthesisFeedbackManager;
    import com.fuzzy.influxdb.oracle.InfluxDBHintOracle;
    import com.fuzzy.iotdb.feedback.IotDBQuerySynthesisFeedbackManager;
    import lombok.extern.slf4j.Slf4j;

    import java.io.File;
    import java.io.FileWriter;
    import java.io.IOException;
    import java.io.Writer;
    import java.nio.file.Files;
    import java.text.DateFormat;
    import java.text.SimpleDateFormat;
    import java.util.*;
    import java.util.concurrent.ExecutorService;
    import java.util.concurrent.Executors;
    import java.util.concurrent.ScheduledExecutorService;
    import java.util.concurrent.TimeUnit;
    import java.util.concurrent.atomic.AtomicBoolean;
    import java.util.concurrent.atomic.AtomicLong;

    // 由GlobalState统串全局
    // 依次详解Main
    // DBMSExecutor.DatabaseProvider -> create globalState
    @Slf4j
    public final class Main {

        public static final File LOG_DIRECTORY = new File("logs");
        public static volatile AtomicLong nrQueries = new AtomicLong();
        public static volatile AtomicLong nrDatabases = new AtomicLong();
        public static volatile AtomicLong nrSuccessfulActions = new AtomicLong();
        public static volatile AtomicLong nrUnsuccessfulActions = new AtomicLong();
        public static volatile AtomicLong threadsShutdown = new AtomicLong();
        static boolean progressMonitorStarted;

        static {
            System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "ERROR");
            if (!LOG_DIRECTORY.exists()) {
                LOG_DIRECTORY.mkdir();
            }
        }

        private Main() {
        }

        public static final class StateLogger {

            private final File loggerFile;
            private File curFile;
            private File queryPlanFile;
            private File syntaxErrorFile;
            private FileWriter logFileWriter;
            public FileWriter currentFileWriter;
            private FileWriter queryPlanFileWriter;
            private FileWriter logSyntaxErrorQueryWriter;
            private static final List<String> INITIALIZED_PROVIDER_NAMES = new ArrayList<>();
            private final boolean logEachSelect;
            private final boolean logQueryPlan;
            private final boolean logSyntaxErrorQuery;
            private final DatabaseProvider<?, ?, ?> databaseProvider;

            private static final class AlsoWriteToConsoleFileWriter extends FileWriter {

                AlsoWriteToConsoleFileWriter(File file) throws IOException {
                    super(file);
                }

                @Override
                public Writer append(CharSequence arg0) throws IOException {
                    System.err.println(arg0);
                    return super.append(arg0);
                }

                @Override
                public void write(String str) throws IOException {
                    System.err.println(str);
                    super.write(str);
                }
            }

            public StateLogger(String databaseName, DatabaseProvider<?, ?, ?> provider, MainOptions options) {
                File dir = new File(LOG_DIRECTORY, provider.getDBMSName());
                if (dir.exists() && !dir.isDirectory()) {
                    throw new AssertionError(dir);
                }
                ensureExistsAndIsEmpty(dir, provider);
                loggerFile = new File(dir, databaseName + ".log");
                logEachSelect = options.logEachSelect();
                if (logEachSelect) {
                    curFile = new File(dir, databaseName + "-cur.log");
                }
                logQueryPlan = options.logQueryPlan();
                if (logQueryPlan) {
                    queryPlanFile = new File(dir, databaseName + "-plan.log");
                }
                logSyntaxErrorQuery = options.logSyntaxErrorQuery();
                if (logSyntaxErrorQuery) syntaxErrorFile = new File(dir, databaseName + "-syntax.log");
                this.databaseProvider = provider;
            }

            private void ensureExistsAndIsEmpty(File dir, DatabaseProvider<?, ?, ?> provider) {
                if (INITIALIZED_PROVIDER_NAMES.contains(provider.getDBMSName())) {
                    return;
                }
                synchronized (INITIALIZED_PROVIDER_NAMES) {
                    if (!dir.exists()) {
                        try {
                            Files.createDirectories(dir.toPath());
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    }
                    File[] listFiles = dir.listFiles();
                    assert listFiles != null : "directory was just created, so it should exist";
                    for (File file : listFiles) {
                        if (!file.isDirectory()) {
                            file.delete();
                        }
                    }
                    INITIALIZED_PROVIDER_NAMES.add(provider.getDBMSName());
                }
            }

            private FileWriter getLogFileWriter() {
                if (logFileWriter == null) {
                    try {
                        logFileWriter = new AlsoWriteToConsoleFileWriter(loggerFile);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
                return logFileWriter;
            }

            public FileWriter getCurrentFileWriter() {
                if (!logEachSelect) {
                    throw new UnsupportedOperationException();
                }
                if (currentFileWriter == null) {
                    try {
                        currentFileWriter = new FileWriter(curFile, false);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
                return currentFileWriter;
            }

            public FileWriter getQueryPlanFileWriter() {
                if (!logQueryPlan) {
                    throw new UnsupportedOperationException();
                }
                if (queryPlanFileWriter == null) {
                    try {
                        queryPlanFileWriter = new FileWriter(queryPlanFile, true);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
                return queryPlanFileWriter;
            }

            public FileWriter getSyntaxErrorQueryWriter() {
                if (!logSyntaxErrorQuery) {
                    throw new UnsupportedOperationException();
                }
                if (logSyntaxErrorQueryWriter == null) {
                    try {
                        logSyntaxErrorQueryWriter = new FileWriter(syntaxErrorFile, true);
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
                return logSyntaxErrorQueryWriter;
            }

            public void writeCurrent(StateToReproduce state) {
                if (!logEachSelect) {
                    throw new UnsupportedOperationException();
                }
                printState(getCurrentFileWriter(), state);
                try {
                    currentFileWriter.flush();

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            public void writeCurrent(String input) {
                write(databaseProvider.getLoggableFactory().createLoggable(input));
            }

            public void writeCurrentNoLineBreak(String input) {
                write(databaseProvider.getLoggableFactory().createLoggableWithNoLinebreak(input));
            }

            private void write(Loggable loggable) {
                if (!logEachSelect) {
                    throw new UnsupportedOperationException();
                }
                try {
                    getCurrentFileWriter().write(loggable.getLogString());

                    currentFileWriter.flush();
                } catch (IOException e) {
                    throw new AssertionError();
                }
            }

            public void writeQueryPlan(String queryPlan) {
                if (!logQueryPlan) {
                    throw new UnsupportedOperationException();
                }
                try {
                    getQueryPlanFileWriter().append(removeNamesFromQueryPlans(queryPlan));
                    queryPlanFileWriter.flush();
                } catch (IOException e) {
                    throw new AssertionError();
                }
            }

            public void logException(Throwable reduce, StateToReproduce state) {
                Loggable stackTrace = getStackTrace(reduce);
                FileWriter logFileWriter2 = getLogFileWriter();
                try {
                    logFileWriter2.write(stackTrace.getLogString());
                    printState(logFileWriter2, state);
                } catch (IOException e) {
                    throw new AssertionError(e);
                } finally {
                    try {
                        logFileWriter2.flush();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

            private Loggable getStackTrace(Throwable e1) {
                return databaseProvider.getLoggableFactory().convertStacktraceToLoggable(e1);
            }

            private void printState(FileWriter writer, StateToReproduce state) {
                StringBuilder sb = new StringBuilder();

                sb.append(databaseProvider.getLoggableFactory()
                        .getInfo(state.getDatabaseName(), state.getDatabaseVersion(), state.getSeedValue()).getLogString());

                for (Query<?> s : state.getStatements()) {
                    sb.append(databaseProvider.getLoggableFactory().createLoggable(s.getLogString()).getLogString());
                }
                try {
                    String str = sb.toString().replace("\\n", "\n");
                    writer.write(str);
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }

            public void writeSyntaxErrorQuery(String syntaxErrorQuery) {
                if (!logSyntaxErrorQuery) {
                    throw new UnsupportedOperationException();
                }
                try {
                    getSyntaxErrorQueryWriter().append(syntaxErrorQuery)
                            .append(System.lineSeparator())
                            .append(System.lineSeparator());
                    getSyntaxErrorQueryWriter().flush();
                } catch (IOException e) {
                    throw new AssertionError();
                }
            }

            private String removeNamesFromQueryPlans(String queryPlan) {
                String result = queryPlan;
                result = result.replaceAll("t[0-9]+", "t0"); // Avoid duplicate tables
                result = result.replaceAll("v[0-9]+", "v0"); // Avoid duplicate views
                result = result.replaceAll("i[0-9]+", "i0"); // Avoid duplicate indexes
                return result + "\n";
            }
        }

        public static class QueryManager<C extends TSFuzzyDBConnection> {

            private final GlobalState<?, ?, C> globalState;

            QueryManager(GlobalState<?, ?, C> globalState) {
                this.globalState = globalState;
            }

            public boolean execute(Query<C> q, String... fills) throws Exception {
                boolean success;
                success = q.execute(globalState, fills);
                Main.nrSuccessfulActions.addAndGet(1);
                if (globalState.getOptions().loggerPrintFailed() || success) {
                    globalState.getState().logStatement(q);
                }
                return success;
            }

            public DBValResultSet executeAndGet(Query<C> q, String... fills) throws Exception {
                globalState.getState().logStatement(q);
                DBValResultSet result;
                result = q.executeAndGet(globalState, fills);
                Main.nrSuccessfulActions.addAndGet(1);
                return result;
            }

            public void incrementSelectQueryCount() {
                Main.nrQueries.addAndGet(1);
            }

            public Long getSelectQueryCount() {
                return Main.nrQueries.get();
            }

            public void incrementCreateDatabase() {
                Main.nrDatabases.addAndGet(1);
            }

        }

        public static void main(String[] args) {
            System.exit(executeMain(args));
        }

        public static class DBMSExecutor<G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends TSFuzzyDBConnection> {

            private final DatabaseProvider<G, O, C> provider;
            private final MainOptions options;
            private final O command;
            private final String databaseName;
            private StateLogger logger;
            private StateToReproduce stateToRepro;
            private final Randomly r;

            public DBMSExecutor(DatabaseProvider<G, O, C> provider, MainOptions options, O dbmsSpecificOptions,
                                String databaseName, Randomly r) {
                this.provider = provider;
                this.options = options;
                this.databaseName = databaseName;
                this.command = dbmsSpecificOptions;
                this.r = r;
            }

            private G createGlobalState() {
                try {
                    return provider.getGlobalStateClass().getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }

            public O getCommand() {
                return command;
            }

            public void testConnection() throws Exception {
                G state = getInitializedGlobalState(options.getRandomSeed());
                try (C con = provider.createDatabase(state)) {
                    QueryManager<C> manager = new QueryManager<>(state);
                    try {
                        stateToRepro.databaseVersion = con.getDatabaseVersion();
                    } catch (Exception e) {
                        // ignore
                    }
                    state.setConnection(con);
                    state.setStateLogger(logger);
                    state.setManager(manager);
                    if (state.getOptions().isDropDatabase()) provider.dropDatabase(state);
                    return;
                }
            }

            public void run() throws Exception {
                G state = createGlobalState();
                stateToRepro = provider.getStateToReproduce(databaseName);
                stateToRepro.seedValue = r.getSeed();
                state.setState(stateToRepro);
                logger = new StateLogger(databaseName, provider, options);
                state.setRandomly(r);
                state.setDatabaseName(databaseName);
                state.setMainOptions(options);
                state.setDbmsSpecificOptions(command);
                try (C con = provider.createDatabase(state)) {
                    QueryManager<C> manager = new QueryManager<>(state);
                    try {
                        stateToRepro.databaseVersion = con.getDatabaseVersion();
                    } catch (Exception e) {
                        // ignore
                    }
                    state.setConnection(con);
                    state.setStateLogger(logger);
                    state.setManager(manager);
                    if (options.logEachSelect()) {
                        logger.writeCurrent(state.getState());
                    }
                    Reproducer<G> reproducer = null;
                    if (options.enableQPG()) {
                        provider.generateAndTestDatabaseWithQueryPlanGuidance(state);
                    } else {
                        reproducer = provider.generateAndTestDatabase(state);
                    }
                    try {
                        logger.getCurrentFileWriter().close();
                        logger.currentFileWriter = null;
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                    if (reproducer != null && options.useReducer()) {
                        System.out.println("EXPERIMENTAL: Trying to reduce queries using a simple reducer.");
                        System.out.println("Reduced query will be output to stdout but not logs.");
                        G newGlobalState = createGlobalState();
                        newGlobalState.setState(stateToRepro);
                        newGlobalState.setRandomly(r);
                        newGlobalState.setDatabaseName(databaseName);
                        newGlobalState.setMainOptions(options);
                        newGlobalState.setDbmsSpecificOptions(command);
                        QueryManager<C> newManager = new QueryManager<>(newGlobalState);
                        newGlobalState.setStateLogger(new StateLogger(databaseName, provider, options));
                        newGlobalState.setManager(newManager);

                        Reducer<G> reducer = new StatementReducer<>(provider);
                        reducer.reduce(state, reproducer, newGlobalState);
                        throw new AssertionError("Found a potential bug");
                    }
                }
            }

            private G getInitializedGlobalState(long seed) {
                G state = createGlobalState();
                stateToRepro = provider.getStateToReproduce(databaseName);
                stateToRepro.seedValue = seed;
                state.setState(stateToRepro);
                logger = new StateLogger(databaseName, provider, options);
                Randomly r = new Randomly(seed);
                state.setRandomly(r);
                state.setDatabaseName(databaseName);
                state.setMainOptions(options);
                state.setDbmsSpecificOptions(command);
                return state;
            }

            public StateLogger getLogger() {
                return logger;
            }

            public StateToReproduce getStateToReproduce() {
                return stateToRepro;
            }
        }

        public static class DBMSExecutorFactory<G extends GlobalState<O, ?, C>, O extends DBMSSpecificOptions<?>, C extends TSFuzzyDBConnection> {

            private final DatabaseProvider<G, O, C> provider;
            private final MainOptions options;
            private final O command;

            public DBMSExecutorFactory(DatabaseProvider<G, O, C> provider, MainOptions options) {
                this.provider = provider;
                this.options = options;
                this.command = createCommand();

            }

            private O createCommand() {
                try {
                    return provider.getOptionClass().getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }

            public O getCommand() {
                return command;
            }

            @SuppressWarnings("unchecked")
            public DBMSExecutor<G, O, C> getDBMSExecutor(String databaseName, Randomly r) {
                try {
                    return new DBMSExecutor<G, O, C>(provider.getClass().getDeclaredConstructor().newInstance(), options,
                            command, databaseName, r);
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }

            public DatabaseProvider<G, O, C> getProvider() {
                return provider;
            }

        }


        JCommander jc = commandBuilder.programName("TSGuard").build();
        jc.parse(args);


        public static int executeMain(String... args) throws AssertionError {
            List<DatabaseProvider<?, ?, ?>> providers = getDBMSProviders();
            Map<String, DBMSExecutorFactory<?, ?, ?>> nameToProvider = new HashMap<>();
            MainOptions options = new MainOptions();
            Builder commandBuilder = JCommander.newBuilder().addObject(options);
            for (DatabaseProvider<?, ?, ?> provider : providers) {
                String name = provider.getDBMSName();
                DBMSExecutorFactory<?, ?, ?> executorFactory = new DBMSExecutorFactory<>(provider, options);
                commandBuilder = commandBuilder.addCommand(name, executorFactory.getCommand());
                nameToProvider.put(name, executorFactory);
            }
            JCommander jc = commandBuilder.programName("SQLancer").build();
            jc.parse(args);

            if (jc.getParsedCommand() == null || options.isHelp()) {
                jc.usage();
                return options.getErrorExitCode();
            }

            Randomly.initialize(options);
            if (options.getSamplingFrequency() < 1) throw new AssertionError("SamplingFrequency must >= 1");
            if (options.printProgressInformation()) {
                startProgressMonitor();
                if (options.printProgressSummary()) {
                    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

                        @Override
                        public void run() {
                            System.out.println("Overall execution statistics");
                            System.out.println("============================");
                            System.out.println(formatInteger(nrQueries.get()) + " queries");
                            System.out.println(formatInteger(nrDatabases.get()) + " databases");
                            System.out.println(
                                    formatInteger(nrSuccessfulActions.get()) + " successfully-executed statements");
                            System.out.println(
                                    formatInteger(nrUnsuccessfulActions.get()) + " unsuccessfuly-executed statements");
                        }

                        private String formatInteger(long intValue) {
                            if (intValue > 1000) {
                                return String.format("%,9dk", intValue / 1000);
                            } else {
                                return String.format("%,10d", intValue);
                            }
                        }
                    }));
                }
            }



            ExecutorService execService = Executors.newFixedThreadPool(options.getNumberConcurrentThreads());
            DBMSExecutorFactory<?, ?, ?> executorFactory = nameToProvider.get(jc.getParsedCommand());

            if (options.performConnectionTest()) {
                try {
                    executorFactory.getDBMSExecutor(options.getDatabasePrefix() + "connectiontest", new Randomly())
                            .testConnection();
                } catch (Exception e) {
                    System.err.println(
                            "SQLancer failed creating a test database, indicating that SQLancer might have failed connecting to the DBMS. In order to change the username, password, host and port, you can use the --username, --password, --host and --port options.\n\n");
                    e.printStackTrace();
                    return options.getErrorExitCode();
                }
            }
            final AtomicBoolean someOneFails = new AtomicBoolean(false);


        if (options.performConnectionTest()) {
            try {
                executorFactory.getDBMSExecutor(options.getDatabasePrefix() + "connectiontest", new Randomly())
                        .testConnection();
            } catch (Exception e) {
                System.err.println(
                        "failed creating a test database, indicating that might have failed connecting to the DBMS. In order to change the username, password, host and port, you can use the --username, --password, --host and --port options.\n\n");
                return options.getErrorExitCode();
            }
        }
        final AtomicBoolean someOneFails = new AtomicBoolean(false);

        for (int i = 0; i < options.getTotalNumberTries(); i++) {
            String databaseName = generateNameForDatabase(jc.getParsedCommand(), options.getDatabasePrefix(), i);


            for (int i = 0; i < options.getTotalNumberTries(); i++) {
                String databaseName;
                // IotDB不支持纯数字作为数据库名
                if (GlobalConstant.IOTDB_DATABASE_NAME.equalsIgnoreCase(jc.getParsedCommand())) {
                    databaseName = options.getDatabasePrefix() + "db" + i;
                } else {
                    databaseName = options.getDatabasePrefix() + i;
                }

                final long seed;
                if (options.getRandomSeed() == -1) {
                    seed = System.currentTimeMillis() + i;
                } else {
                    seed = options.getRandomSeed() + i;
                }
                execService.execute(new Runnable() {

                    @Override
                    public void run() {
                        Thread.currentThread().setName(databaseName);
                        runThread(databaseName);
                    }

                    private void runThread(final String databaseName) {
                        Randomly r = new Randomly(seed);
                        try {
                            int maxNrDbs = options.getMaxGeneratedDatabases();
                            // run without a limit if maxNrDbs == -1
                            for (int i = 0; i < maxNrDbs || maxNrDbs == -1; i++) {
                                Boolean continueRunning = run(options, execService, executorFactory, r, databaseName);
                                if (!continueRunning) {
                                    someOneFails.set(true);
                                    break;
                                }
                            }
                        } finally {
                            threadsShutdown.addAndGet(1);
                            if (threadsShutdown.get() == options.getTotalNumberTries()) {
                                execService.shutdown();
                            }
                        }
                    }

                    private boolean run(MainOptions options, ExecutorService execService,
                                        DBMSExecutorFactory<?, ?, ?> executorFactory, Randomly r, final String databaseName) {
                        DBMSExecutor<?, ?, ?> executor = executorFactory.getDBMSExecutor(databaseName, r);
                        try {
                            executor.run();
                            return true;
                        } catch (IgnoreMeException e) {
                            return true;
                        } catch (Throwable reduce) {
    //                        reduce.printStackTrace();
                            executor.getStateToReproduce().exception = reduce.getMessage();
                            executor.getLogger().logFileWriter = null;
                            executor.getLogger().logException(reduce, executor.getStateToReproduce());
                            return false;
                        } finally {
                            try {
                                if (options.logEachSelect()) {
                                    if (executor.getLogger().currentFileWriter != null) {
                                        executor.getLogger().currentFileWriter.close();
                                    }
                                    executor.getLogger().currentFileWriter = null;
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
            }
            try {
                if (options.getTimeoutSeconds() == -1) {
                    execService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
                } else {
                    execService.awaitTermination(options.getTimeoutSeconds(), TimeUnit.SECONDS);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                execService.shutdownNow();
            }

            StringBuilder sb = new StringBuilder();
            sb.append(String.format("nrQueries:%d\n", nrQueries.get()));
            sb.append(String.format("nrDatabases:%d\n", nrDatabases.get()));
            sb.append(String.format("nrSuccessfulActions:%d\n", nrSuccessfulActions.get()));
            sb.append(String.format("nrUnsuccessfulActions:%d\n", nrUnsuccessfulActions.get()));
            // TDengine
            // record expression depth
            sb.append(String.format("TDengine Expression iteration depth:%d\n", TDengineQuerySynthesisFeedbackManager.expressionDepth.get()));


            // query execution statistical
            sb.append(String.format("Query statistics:%s\n", TDengineQuerySynthesisFeedbackManager.queryExecutionStatistical));

            // query syntax sequence number
            sb.append(String.format("Number of query syntax sequences:%s\n", TDengineQuerySynthesisFeedbackManager.querySynthesisFeedback.getSequenceNumber()));

            sb.append(String.format("invalidQueryCounter:%s\n", ConstraintValue.counter));

            // query syntax sequence
            sb.append(TDengineQuerySynthesisFeedbackManager.querySynthesisFeedback.toString()).append("\n");

            // IotDB
            // record expression depth
            sb.append(String.format("IotDB Expression iteration depth:%d\n", IotDBQuerySynthesisFeedbackManager.expressionDepth.get()));

            // query execution statistical
            sb.append(String.format("Query statistics:%s\n", IotDBQuerySynthesisFeedbackManager.queryExecutionStatistical));

            // query syntax sequence number
            sb.append(String.format("Number of query syntax sequences:%s\n", IotDBQuerySynthesisFeedbackManager.querySynthesisFeedback.getSequenceNumber()));

            sb.append(String.format("invalidQueryCounter:%s\n", ConstraintValue.counter));

            // query syntax sequence
            sb.append(IotDBQuerySynthesisFeedbackManager.querySynthesisFeedback.toString()).append("\n");

            // InfluxDB
            // record expression depth
            sb.append(String.format("InfluxDB Expression iteration depth:%d\n", InfluxDBQuerySynthesisFeedbackManager.expressionDepth.get()));

            // query execution statistical
            sb.append(String.format("Query statistics:%s\n", InfluxDBQuerySynthesisFeedbackManager.queryExecutionStatistical));

            // query syntax sequence number
            sb.append(String.format("Number of query syntax sequences:%s\n", InfluxDBQuerySynthesisFeedbackManager.querySynthesisFeedback.getSequenceNumber()));

            sb.append(String.format("invalidQueryCounter:%s\n", ConstraintValue.counter));

            sb.append(String.format("nrHintQueries:%d\n",
                    InfluxDBHintOracle.nrHintQueries.get()));
            sb.append(String.format("nrHintMismatches:%d\n",
                    InfluxDBHintOracle.nrHintMismatches.get()));

            // query syntax sequence
            sb.append(InfluxDBQuerySynthesisFeedbackManager.querySynthesisFeedback.toString()).append("\n");
            try (FileWriter writer = new FileWriter("C:\\Users\\timmy\\Desktop\\TSGuard\\TSGuard-Hint\\TSGuard\\tsFuzzy\\logs\\statistic.info")) {
                //C:\Users\timmy\Desktop\TSGuard\TSGuard-Detecting-Logic-Bugs-in-Time-Series-Management-Systems-via-Time-Series-Algebra\TSGuard\tsFuzzy\logs
                writer.write(sb.toString());
            } catch (IOException e) {
                e.printStackTrace();
            }


            return someOneFails.get() ? options.getErrorExitCode() : 0;
        }

        /**
         * To register a new provider, it is necessary to implement the DatabaseProvider interface and add an additional
         * configuration file, see https://docs.oracle.com/javase/9/docs/api/java/util/ServiceLoader.html. Currently, we use
         * an @AutoService annotation to create the configuration file automatically. This allows SQLancer to pick up
         * providers in other JARs on the classpath.
         *
         * @return The list of service providers on the classpath
         */
        static List<DatabaseProvider<?, ?, ?>> getDBMSProviders() {
            List<DatabaseProvider<?, ?, ?>> providers = new ArrayList<>();
            @SuppressWarnings("rawtypes")
            ServiceLoader<DatabaseProvider> loader = ServiceLoader.load(DatabaseProvider.class);
            for (DatabaseProvider<?, ?, ?> provider : loader) {
                providers.add(provider);
            }
            checkForIssue799(providers);
            return providers;
        }

        // see https://github.com/sqlancer/sqlancer/issues/799
        private static void checkForIssue799(List<DatabaseProvider<?, ?, ?>> providers) {
            if (providers.isEmpty()) {
                System.err.println(
                        "No DBMS implementations (i.e., instantiations of the DatabaseProvider class) were found. You likely ran into an issue described in https://github.com/sqlancer/sqlancer/issues/799. As a workaround, I now statically load all supported providers as of June 7, 2023.");
                // TODO
    //            providers.add(new ArangoDBProvider());
    //            providers.add(new CitusProvider());
    //            providers.add(new ClickHouseProvider());
    //            providers.add(new CnosDBProvider());
    //            providers.add(new CockroachDBProvider());
    //            providers.add(new CosmosProvider());
    //            providers.add(new DatabendProvider());
    //            providers.add(new DorisProvider());
    //            providers.add(new DuckDBProvider());
    //            providers.add(new H2Provider());
    //            providers.add(new HSQLDBProvider());
    //            providers.add(new MariaDBProvider());
    //            providers.add(new MaterializeProvider());
    //            providers.add(new MongoDBProvider());
    //            providers.add(new MySQLProvider());
    //            providers.add(new OceanBaseProvider());
    //            providers.add(new PostgresProvider());
    //            providers.add(new QuestDBProvider());
    //            providers.add(new SQLite3Provider());
    //            providers.add(new StoneDBProvider());
    //            providers.add(new TiDBProvider());
    //            providers.add(new TimescaleDBProvider());
    //            providers.add(new YCQLProvider());
    //            providers.add(new YSQLProvider());
            }
        }

        private static synchronized void startProgressMonitor() {
            if (progressMonitorStarted) {
                /*
                 * it might be already started if, for example, the main method is called multiple times in a test (see
                 * https://github.com/sqlancer/sqlancer/issues/90).
                 */
                return;
            } else {
                progressMonitorStarted = true;
            }
            final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.scheduleAtFixedRate(new Runnable() {

                private long timeMillis = System.currentTimeMillis();
                private long lastNrQueries;
                private long lastNrDbs;

                {
                    timeMillis = System.currentTimeMillis();
                }

                @Override
                public void run() {
                    long elapsedTimeMillis = System.currentTimeMillis() - timeMillis;
                    long currentNrQueries = nrQueries.get();
                    long nrCurrentQueries = currentNrQueries - lastNrQueries;
                    double throughput = nrCurrentQueries / (elapsedTimeMillis / 1000d);
                    long currentNrDbs = nrDatabases.get();
                    long nrCurrentDbs = currentNrDbs - lastNrDbs;
                    double throughputDbs = nrCurrentDbs / (elapsedTimeMillis / 1000d);
                    long successfulStatementsRatio = (long) (100.0 * nrSuccessfulActions.get()
                            / (nrSuccessfulActions.get() + nrUnsuccessfulActions.get()));
                    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
                    Date date = new Date();
                    System.out.println(String.format(
                            "[%s] Executed %d queries (%d queries/s; %.2f/s dbs, successful statements: %2d%%). Threads shut down: %d.",
                            dateFormat.format(date), currentNrQueries, (int) throughput, throughputDbs,
                            successfulStatementsRatio, threadsShutdown.get()));
                    timeMillis = System.currentTimeMillis();
                    lastNrQueries = currentNrQueries;
                    lastNrDbs = currentNrDbs;
                }
            }, 5, 5, TimeUnit.SECONDS);


    private static String generateNameForDatabase(String databaseType, String databaseNamePrefix, int iterationNo) {
        String databaseName;

        if (GlobalConstant.IOTDB_DATABASE_NAME.equalsIgnoreCase(databaseType)) {
            // IotDB 不支持纯数字作为数据库名
            databaseName = databaseNamePrefix + "db" + iterationNo;
        } else if (GlobalConstant.PROMETHEUS_DATABASE_NAME.equalsIgnoreCase(databaseType)) {
            // Prometheus 要求随机生成数据库名, 基于此使用 Remote Write 时不限于时间范围
            databaseName = databaseNamePrefix + "_" + iterationNo + "_"
                    + UUID.randomUUID().toString().replace("-", "_");
        } else {
            databaseName = databaseNamePrefix + iterationNo;
        }
        return databaseName;
    }

    /**
     * To register a new provider, it is necessary to implement the DatabaseProvider interface and add an additional
     * configuration file, see https://docs.oracle.com/javase/9/docs/api/java/util/ServiceLoader.html. Currently, we use
     * an @AutoService annotation to create the configuration file automatically. This allows TSGuard to pick up
     * providers in other JARs on the classpath.
     *
     * @return The list of service providers on the classpath
     */
    static List<DatabaseProvider<?, ?, ?>> getDBMSProviders() {
        List<DatabaseProvider<?, ?, ?>> providers = new ArrayList<>();
        @SuppressWarnings("rawtypes")
        ServiceLoader<DatabaseProvider> loader = ServiceLoader.load(DatabaseProvider.class);
        for (DatabaseProvider<?, ?, ?> provider : loader) {
            providers.add(provider);
        }
        return providers;
    }

    private static synchronized void startProgressMonitor() {
        if (progressMonitorStarted) {
            /*
             * it might be already started if, for example, the main method is called multiple times in a test (see
             * https://github.com/sqlancer/sqlancer/issues/90).
             */
            return;
        } else {
            progressMonitorStarted = true;

        }

    }
