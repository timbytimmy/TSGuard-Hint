package com.fuzzy.griddb;

import com.fuzzy.*;
import com.fuzzy.common.DBMSCommon;
import com.fuzzy.common.constant.GlobalConstant;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.common.query.SQLQueryProvider;
import com.fuzzy.griddb.feedback.GridDBQuerySynthesisFeedbackManager;
import com.fuzzy.griddb.gen.GridDBIndexGenerator;
import com.fuzzy.griddb.gen.GridDBInsertGenerator;
import com.fuzzy.griddb.gen.GridDBTableGenerator;
import com.google.auto.service.AutoService;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@AutoService(DatabaseProvider.class)
public class GridDBProvider extends SQLProviderAdapter<GridDBGlobalState, GridDBOptions> {

    public GridDBProvider() {
        super(GridDBGlobalState.class, GridDBOptions.class);
    }

    enum Action implements AbstractAction<GridDBGlobalState> {
//        SHOW_DATABASES((g) -> new SQLQueryAdapter("SHOWDATABASE")),
//        SHOW_TABLES((g) -> new SQLQueryAdapter("SHOWTABLE")),
        INSERT(GridDBInsertGenerator::insertRow),
        INDEX(GridDBIndexGenerator::createIndex),
        CREATE_TABLE((g) -> {
            String tableName = DBMSCommon.createTableName(g.getSchema().getMaxTableIndex() + 1);
            return GridDBTableGenerator.generate(g, tableName);
        })
        ;

        private final SQLQueryProvider<GridDBGlobalState> sqlQueryProvider;

        Action(SQLQueryProvider<GridDBGlobalState> sqlQueryProvider) {
            this.sqlQueryProvider = sqlQueryProvider;
        }

        @Override
        public SQLQueryAdapter getQuery(GridDBGlobalState globalState) throws Exception {
            return sqlQueryProvider.getQuery(globalState);
        }
    }

    private static int mapActions(GridDBGlobalState globalState, Action a) {
        Randomly r = globalState.getRandomly();
        int nrPerformed = 0;
        switch (a) {
//            case SHOW_TABLES:
//            case SHOW_DATABASES:
//                nrPerformed = r.getInteger(0, 1);
//                break;
            case INSERT:
                nrPerformed = r.getInteger(5, globalState.getOptions().getMaxNumberInserts());
                break;
            case INDEX:
                nrPerformed = r.getInteger(0, 4);
                break;
            case CREATE_TABLE:
                nrPerformed = r.getInteger(0, 2);
                break;
            default:
                throw new AssertionError(a);
        }
        return nrPerformed;
    }

    @Override
    protected void recordQueryExecutionStatistical(GridDBGlobalState globalState) {
        // record expression depth
        globalState.getLogger().writeSyntaxErrorQuery(String.format("表达式迭代深度:%d",
                GridDBQuerySynthesisFeedbackManager.expressionDepth.get()));

        // query execution statistical
        globalState.getLogger().writeSyntaxErrorQuery(String.format("查询统计信息:%s",
                GridDBQuerySynthesisFeedbackManager.queryExecutionStatistical));

        // query syntax sequence number
        globalState.getLogger().writeSyntaxErrorQuery(String.format("查询语法序列数目:%s",
                GridDBQuerySynthesisFeedbackManager.querySynthesisFeedback.getSequenceNumber()));

        // query syntax sequence
        globalState.getLogger().writeSyntaxErrorQuery(
                GridDBQuerySynthesisFeedbackManager.querySynthesisFeedback.toString());
    }

    @Override
    public void generateDatabase(GridDBGlobalState globalState) throws Exception {
        while (globalState.getSchema().getDatabaseTables().size() < Randomly.smallNumber() + 1) {
            String tableName = DBMSCommon.createTableName(globalState.getSchema().getMaxTableIndex() + 1);
            SQLQueryAdapter createTable = GridDBTableGenerator.generate(globalState, tableName);
            globalState.executeStatement(createTable);
        }

        StatementExecutor<GridDBGlobalState, Action> se = new StatementExecutor<>(globalState, Action.values(),
                GridDBProvider::mapActions, (q) -> {
            if (globalState.getSchema().getDatabaseTables().isEmpty()) {
                throw new IgnoreMeException();
            }
        });
        se.executeStatements();
    }

    @Override
    public SQLConnection createDatabase(GridDBGlobalState globalState) throws SQLException {
        String host = globalState.getOptions().getHost();
        int port = globalState.getOptions().getPort();
//        String precision = globalState.getOptions().getPrecision();
//        if (StringUtils.isBlank(precision) || !GridDBPrecision.isExist(precision))
//            precision = GridDBPrecision.getRandom().name();
        if (host == null) host = GridDBOptions.DEFAULT_HOST;
        if (port == MainOptions.NO_SET_PORT) port = GridDBOptions.DEFAULT_PORT;
        String userName = globalState.getOptions().getUserName();
        String password = globalState.getOptions().getPassword();
        // GridDB单库单表
        String databaseName = globalState.getDatabaseName();

        GridDBConnection gridDBConnection = new GridDBConnection(host, port, userName, password);
        List<String> tableNamesInDatabase = getTableNamesInDatabase(gridDBConnection, databaseName);
        try (GridDBStatement s = gridDBConnection.createStatement()) {
            // 删除database下全部table
            for (String tableName : tableNamesInDatabase) {
                s.execute("DROP TABLE IF EXISTS " + tableName);
            }
            SQLQueryAdapter createTableQuery = GridDBTableGenerator.generate(globalState, "t0");
            globalState.getState().logStatement(createTableQuery);
            s.execute(createTableQuery.getQueryString());
        }

        if (gridDBConnection.isClosed()) {
            throw new SQLException("createDatabase error...");
        }
        return new SQLConnection(gridDBConnection);
    }

    private List<String> getTableNamesInDatabase(GridDBConnection GridDBConnection, String databaseName)
            throws SQLException {
        List<String> tableNames = new ArrayList<>();
        DatabaseMetaData metaData = GridDBConnection.getMetaData();
        try (ResultSet tables = metaData.getTables(null, null,
                String.format("%s_%%", databaseName), new String[]{"TABLE"})) {
            while (tables.next()) {
                tableNames.add(tables.getString("TABLE_NAME"));
            }
        }
        return tableNames;
    }

    @Override
    public void dropDatabase(GridDBGlobalState globalState) throws Exception {
        List<GridDBSchema.GridDBTable> databaseTables = globalState.getSchema().getDatabaseTables();
        for (GridDBSchema.GridDBTable table : databaseTables) {
            globalState.executeStatement(new SQLQueryAdapter("DROP TABLE IF EXISTS " + table.getName()));
        }
    }

    @Override
    public String getDBMSName() {
        return GlobalConstant.GRIDDB_DATABASE_NAME;
    }

    @Override
    public boolean addRowsToAllTables(GridDBGlobalState globalState) throws Exception {
        List<GridDBSchema.GridDBTable> tablesNoRow = globalState.getSchema().getDatabaseTables().stream()
                .filter(t -> t.getNrRows(globalState) == 0).collect(Collectors.toList());
        for (GridDBSchema.GridDBTable table : tablesNoRow) {
            SQLQueryAdapter queryAddRows = GridDBInsertGenerator.insertRow(globalState, table);
            globalState.executeStatement(queryAddRows);
        }
        return true;
    }

}
