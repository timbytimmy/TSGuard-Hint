package com.fuzzy.iotdb;


import com.fuzzy.common.query.ExpectedErrors;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class IotDBErrors {

    private IotDBErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("parsing SQL to physical plan");
        errors.add("Invalid input expression");
        errors.add("Unsupported expression type: FUNCTION");
        errors.add("is illegal, identifier not enclosed");
        errors.add("TIMESTAMP does not support IS NULL/IS NOT NULL");
        errors.add("INTERNAL_SERVER_ERROR(305)");
        errors.add("701: Constant column is not supported");
        errors.add("Index 1 out of bounds for length 1");
        errors.add("301:");

        return errors;
    }

    public static List<Pattern> getExpressionRegexErrors() {
        ArrayList<Pattern> errors = new ArrayList<>();

        return errors;
    }

    public static void addExpressionErrors(ExpectedErrors errors) {
        errors.addAll(getExpressionErrors());
        errors.addAllRegexes(getExpressionRegexErrors());
    }

    public static List<String> getInsertUpdateErrors() {
        ArrayList<String> errors = new ArrayList<>();

        // errors.add("doesn't have a default value");

        return errors;
    }

    public static void addInsertUpdateErrors(ExpectedErrors errors) {
        errors.addAll(getInsertUpdateErrors());
    }

}
