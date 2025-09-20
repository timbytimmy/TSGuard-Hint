package com.fuzzy.TDengine;


import com.fuzzy.common.query.ExpectedErrors;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class TDengineErrors {

    private TDengineErrors() {
    }

    public static List<String> getExpressionErrors() {
        ArrayList<String> errors = new ArrayList<>();

        errors.add("syntax error");
        errors.add("Invalid value type: ");
        errors.add("Unexpected generic error");
        errors.add("Invalid operation");

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
