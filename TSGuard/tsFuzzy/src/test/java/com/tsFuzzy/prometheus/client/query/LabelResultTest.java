package com.tsFuzzy.prometheus.client.query;

import com.fuzzy.prometheus.client.converter.ConvertUtil;
import com.fuzzy.prometheus.client.converter.label.DefaultLabelResult;
import junit.framework.TestCase;

public class LabelResultTest extends TestCase {
	private String testLabelData="{\"status\":\"success\",\"data\":[\"person-application-1.5-5dcc65c754-7ztnz\",\"person-application-1.5-5dcc65c754-8gb82\",\"person-application-1.5-5dcc65c754-8xh22\"]}";
	public void testParser() {
		DefaultLabelResult result = ConvertUtil.convertLabelResultString(testLabelData);
		System.out.println("-----" +result.getResult().size());
		for(String data : result.getResult()) {
			System.out.println("=======>" + data);
		}			
	}
}
