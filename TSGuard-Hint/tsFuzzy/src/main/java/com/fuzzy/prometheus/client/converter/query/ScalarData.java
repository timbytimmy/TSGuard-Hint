package com.fuzzy.prometheus.client.converter.query;

import com.fuzzy.prometheus.client.converter.Data;

public class ScalarData extends QueryResultItemValue implements Data {

	public ScalarData(double timestamp, double value) {
		super(timestamp, value);
	}

}
