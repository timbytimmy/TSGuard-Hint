package com.fuzzy.TDengine;

import com.fuzzy.*;
import com.fuzzy.TDengine.feedback.TDengineQuerySynthesisFeedbackManager;
import com.fuzzy.TDengine.gen.TDengineInsertGenerator;
import com.fuzzy.TDengine.gen.TDengineTableGenerator;
import com.fuzzy.TDengine.tsaf.enu.TDenginePrecision;
import com.fuzzy.common.DBMSCommon;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.query.SQLQueryProvider;
import com.google.auto.service.AutoService;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

@AutoService(DatabaseProvider.class)
public class TDengineProvider extends SQLProviderAdapter<TDengineGlobalState, TDengineOptions> {

    public TDengineProvider() {
        super(TDengineGlobalState.class, TDengineOptions.class);
    }

    enum Action implements AbstractAction<TDengineGlobalState> {
        SHOW_DATABASES((g) -> new SQLQueryAdapter("SHOW DATABASES")),
        SHOW_TABLES((g) -> new SQLQueryAdapter("SHOW TABLES")),
        INSERT(TDengineInsertGenerator::insertRow),
        CREATE_TABLE((g) -> {
            String tableName = DBMSCommon.createTableName(g.getSchema().getMaxTableIndex() + 1);
            return TDengineTableGenerator.generate(g, tableName);
        });

        private final SQLQueryProvider<TDengineGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<TDengineGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(TDengineGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(TDengineGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed = 0;
        switch (a) {
            case SHOW_TABLES:
            case SHOW_DATABASES:
                nrPerformed = r.getInteger(0, 1);
                break;
            case INSERT:
                nrPerformed = r.getInteger(5, globalState.getOptions().getMaxNumberInserts());
                break;
            case CREATE_TABLE:
                nrPerformed = r.getInteger(0, 1);
                break;
            default:
                throw new AssertionError(a);
        }
        return nrPerformed;
    }

    @Override
    protected void recordQueryExecutionStatistical(TDengineGlobalState globalState) {
        // record expression depth
        globalState.getLogger().writeSyntaxErrorQuery(String.format("表达式迭代深度:%d",
                TDengineQuerySynthesisFeedbackManager.expressionDepth.get()));

        // query execution statistical
        globalState.getLogger().writeSyntaxErrorQuery(String.format("查询统计信息:%s",
                TDengineQuerySynthesisFeedbackManager.queryExecutionStatistical));

        // query syntax sequence number
        globalState.getLogger().writeSyntaxErrorQuery(String.format("查询语法序列数目:%s",
                TDengineQuerySynthesisFeedbackManager.querySynthesisFeedback.getSequenceNumber()));

        // query syntax sequence
        globalState.getLogger().writeSyntaxErrorQuery(
                TDengineQuerySynthesisFeedbackManager.querySynthesisFeedback.toString());
    }

    @Override
    public void generateDatabase(TDengineGlobalState globalState) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.smallNumber() + 1) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getMaxTableIndex() + 1);
            SQLQueryAdapter createTable = TDengineTableGenerator.generate(globalState, tableName);
            globalState.executeStatement(createTable);
        }

        StatementExecutor<TDengineGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                TDengineProvider::mapActions, (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(TDengineGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        String precision = globalState.getOptions().getPrecision();
        if (StringUtils.isBlank(precision) || !TDenginePrecision.isExist(precision))
            precision = TDenginePrecision.getRandom().name();
        if (host == null) host = TDengineOptions.DEFAULT_HOST;
        if (port == MainOptions.NO_SET_PORT) port = TDengineOptions.DEFAULT_PORT;
        String userName = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        String databaseName = globalState.getDatabaseName();

        TDengineConnection TDengineConnection = new TDengineConnection(host, port, userName, password);
        try (TDengineStatement s = TDengineConnection.createStatement()) {
            s.execute("DROP DATABASE IF EXISTS " + databaseName);
            s.execute(String.format("CREATE DATABASE IF NOT EXISTS %s PRECISION '%s'", databaseName, precision));
            s.execute("USE " + databaseName);
        }

        if (TDengineConnection.isClosed()) {
            throw new SQLException("createDatabase error...");
        }
        return new SQLConnection(TDengineConnection);
    }

    @Override
    public void dropDatabase(TDengineGlobalState globalState) throws Exception {
        globalState.executeStatement(new SQLQueryAdapter("DROP DATABASE IF EXISTS " + globalState.getDatabaseName()));
    }

    @Override
    public String getDBMSName() {
        return GlobalConstant.TDENGINE_DATABASE_NAME;
    }

    @Override
    public boolean addRowsToAllTables(TDengineGlobalState globalState) throws Exception {
        List<TDengineSchema.TDengineTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (TDengineSchema.TDengineTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = TDengineInsertGenerator.insertRow(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

}
