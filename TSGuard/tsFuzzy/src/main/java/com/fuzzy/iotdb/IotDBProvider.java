package com.fuzzy.iotdb;

import com.benchmark.commonClass.TSFuzzyStatement;
import com.fuzzy.*;
import com.fuzzy.common.DBMSCommon;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.query.SQLQueryProvider;
import com.fuzzy.iotdb.feedback.IotDBQuerySynthesisFeedbackManager;
import com.fuzzy.iotdb.gen.*;
import com.fuzzy.iotdb.gen.IotDBShowStatementGenerator.ShowStatementType;
import com.fuzzy.iotdb.resultSet.IotDBResultSet;
import com.google.auto.service.AutoService;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

@AutoService(DatabaseProvider.class)
public class IotDBProvider extends SQLProviderAdapter<IotDBGlobalState, IotDBOptions> {

    public IotDBProvider() {
        super(IotDBGlobalState.class, IotDBOptions.class);
    }

    enum Action implements AbstractAction<IotDBGlobalState> {
        CREATE_TABLE((g) -> {
            String tableName = DBMSCommon.createTableName(g.getSchema().getMaxTableIndex() + 1);
            return IotDBTableGenerator.generate(g, tableName);
        }),
        INSERT(IotDBInsertGenerator::insertRow),
        UPDATE(IotDBUpdateGenerator::replaceRow),
        DELETE(IotDBDeleteGenerator::delete),
        DROP_TABLE(IotDBDropTableGenerator::dropRandomTable),
        // TODO 执行时是否需要确保Show语句必定有返回值?
        SHOW_STATEMENT(IotDBShowStatementGenerator::generateShowStatement);

        private final SQLQueryProvider<IotDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<IotDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(IotDBGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(IotDBGlobalState globalState, IotDBProvider.Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed = 0;
        switch (a) {
            case INSERT:
                nrPerformed = r.getInteger(3, globalState.getOptions().getMaxNumberInserts());
                break;
            case SHOW_STATEMENT:
                nrPerformed = r.getInteger(ShowStatementType.values().length,
                        ShowStatementType.values().length + Randomly.smallNumber());
                break;
            case DELETE:
            case UPDATE:
                // TODO
                nrPerformed = 0;
//                if (globalState.usesPQS())
//                    nrPerformed = r.getInteger(1, globalState.getOptions().getMaxNumberInserts() / 10);
                break;
            case CREATE_TABLE:
                nrPerformed = r.getInteger(0, 2);
                break;
            case DROP_TABLE:
                nrPerformed = 0;
                break;
            default:
                throw new AssertionError(a);
        }
        return nrPerformed;
    }

    @Override
    protected void recordQueryExecutionStatistical(IotDBGlobalState globalState) {
        // record expression depth
        globalState.getLogger().writeSyntaxErrorQuery(String.format("表达式迭代深度:%d",
                IotDBQuerySynthesisFeedbackManager.expressionDepth.get()));

        // query execution statistical
        globalState.getLogger().writeSyntaxErrorQuery(String.format("查询统计信息:%s",
                IotDBQuerySynthesisFeedbackManager.queryExecutionStatistical));

        // query syntax sequence number
        globalState.getLogger().writeSyntaxErrorQuery(String.format("查询语法序列数目:%s",
                IotDBQuerySynthesisFeedbackManager.querySynthesisFeedback.getSequenceNumber()));

        // query syntax sequence
        globalState.getLogger().writeSyntaxErrorQuery(
                IotDBQuerySynthesisFeedbackManager.querySynthesisFeedback.toString());
    }

    @Override
    public void generateDatabase(IotDBGlobalState globalState) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.getNotCachedInteger(1, 2)) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getMaxTableIndex() + 1);
            SQLQueryAdapter createTable = IotDBTableGenerator.generate(globalState, tableName);
            globalState.executeStatement(createTable);
        }

        StatementExecutor<IotDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                IotDBProvider::mapActions, (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(IotDBGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
        if (host == null) host = IotDBOptions.DEFAULT_HOST;
        if (port == MainOptions.NO_SET_PORT) port = IotDBOptions.DEFAULT_PORT;
        String userName = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();

        String databaseName = globalState.getDatabaseName();

        IotDBConnection iotDBConnection = new IotDBConnection(host, port, userName, password);
        try (IotDBStatement s = iotDBConnection.createStatement()) {
            IotDBResultSet iotDBResultSet = (IotDBResultSet) s.executeQuery("SHOW DATABASES " + databaseName);
            if (iotDBResultSet.hasNext()) s.execute("DELETE DATABASE " + databaseName);
            s.execute("CREATE DATABASE " + databaseName);
        }

        if (iotDBConnection.isClosed()) {
            throw new SQLException("createDatabase error...");
        }
        return new SQLConnection(iotDBConnection);
    }

    @Override
    public void dropDatabase(IotDBGlobalState globalState) throws Exception {
        String databaseName = globalState.getDatabaseName();

        try (TSFuzzyStatement s = globalState.getConnection().createStatement()) {
            try (IotDBResultSet iotDBResultSet = (IotDBResultSet)
                    s.executeQuery("SHOW DATABASES " + databaseName)) {
                if (iotDBResultSet.hasNext()) s.execute("DELETE DATABASE " + databaseName);
            }
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public String getDBMSName() {
        return GlobalConstant.IOTDB_DATABASE_NAME;
    }

    @Override
    public boolean addRowsToAllTables(IotDBGlobalState globalState) throws Exception {
        List<IotDBSchema.IotDBTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (IotDBSchema.IotDBTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = IotDBInsertGenerator.insertRow(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

}
