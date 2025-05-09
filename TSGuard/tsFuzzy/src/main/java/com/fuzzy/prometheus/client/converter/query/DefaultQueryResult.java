package com.fuzzy.prometheus.client.converter.query;

import com.fuzzy.prometheus.client.converter.Data;
import com.fuzzy.prometheus.client.converter.Result;

import java.util.ArrayList;
import java.util.List;


public class DefaultQueryResult<T extends Data> extends Result<T>{

	List<T> result = new ArrayList<T>();
	public void addData(T data) {
		result.add(data);
	}
	
	@Override
	public List<T> getResult() {
		return result;
	}

	@Override
	public String toString() {
		return "DefaultQueryResult [result=" + result + "]";
	}
	
	
	
}
