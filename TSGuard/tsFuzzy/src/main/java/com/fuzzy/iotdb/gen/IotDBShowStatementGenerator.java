package com.fuzzy.iotdb.gen;


import com.fuzzy.Randomly;
import com.fuzzy.common.query.ExpectedErrors;
import com.fuzzy.common.query.SQLQueryAdapter;
import com.fuzzy.iotdb.IotDBGlobalState;
import com.fuzzy.iotdb.IotDBVisitor;
import com.fuzzy.iotdb.ast.IotDBConstant;

import java.util.Arrays;

public class IotDBShowStatementGenerator {

    private final StringBuilder sb = new StringBuilder();
    private final Randomly r;
    private final IotDBGlobalState globalState;

    public IotDBShowStatementGenerator(IotDBGlobalState globalState) {
        this.r = globalState.getRandomly();
        this.globalState = globalState;
    }

    public enum ShowStatementType {
        DATABASES,
        TABLES,

    }

    public static SQLQueryAdapter generateShowStatement(IotDBGlobalState globalState) {
        ShowStatementType statementType = Randomly.fromList(Arrays.asList(ShowStatementType.values()));
        switch (statementType) {
            case DATABASES:
                return new IotDBShowStatementGenerator(globalState).generateShowStatement();
            case TABLES:
                return new IotDBShowStatementGenerator(globalState).generateShowTables();
            default:
                throw new IllegalArgumentException(String.format("SHOW语句不存在, 语句类型:%s", statementType));
        }
    }

    private SQLQueryAdapter generateShowStatement() {
        ExpectedErrors errors = new ExpectedErrors();
        sb.append("SHOW DATABASES ");

        switch (globalState.getRandomly().getInteger(1, 4)) {
            case 1:
                sb.append(globalState.getDatabaseName());
                break;
            case 2:
                sb.append("root.*");
                break;
            case 3:
                sb.append("root.**");
                break;
            default:
        }
        return new SQLQueryAdapter(sb.toString(), errors, false);
    }

    public SQLQueryAdapter generateShowTables() {
        ExpectedErrors errors = new ExpectedErrors();
        sb.append("SHOW TIMESERIES ");

        // path
        generateRandomTablePath();
        generateAndAppendLimitClause();

        return new SQLQueryAdapter(sb.toString(), errors, false);
    }

    private void generateRandomTablePath() {
        switch (globalState.getRandomly().getInteger(1, 4)) {
            case 1:
                sb.append(globalState.getDatabaseName());
                if (Randomly.getBoolean())
                    sb.append(".").append(globalState.getSchema().getRandomTable().getName()).append(".")
                            .append(Randomly.fromList(globalState.getSchema().getRandomTable().getColumns()).getName());
                else sb.append(".**");
                break;
            case 2:
                sb.append("root.*");
                break;
            case 3:
                sb.append("root.**");
                break;
            default:
        }
    }

    private void generateAndAppendLimitClause() {
        if (Randomly.getBoolean()) {
            sb.append(" LIMIT ");
            sb.append(IotDBVisitor.asString(IotDBConstant.createIntConstant(Integer.MAX_VALUE)));
            generateAndAppendOffsetClause();
        }
    }

    private void generateAndAppendOffsetClause() {
        if (Randomly.getBoolean()) {
            sb.append(" OFFSET ");
            sb.append(IotDBVisitor.asString(IotDBConstant.createIntConstantNotAsBoolean(0)));
        }
    }
}
