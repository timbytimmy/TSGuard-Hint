package com.fuzzy;

public interface TSFuzzyDBConnection extends AutoCloseable {
    String getDatabaseVersion() throws Exception;
}
