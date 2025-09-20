package com.tsFuzzy.prometheus.client.query;

import com.fuzzy.prometheus.client.converter.ConvertUtil;
import com.fuzzy.prometheus.client.converter.Data;
import com.fuzzy.prometheus.client.converter.query.DefaultQueryResult;
import com.fuzzy.prometheus.client.converter.query.VectorData;
import junit.framework.TestCase;

public class ScalarResultTest extends TestCase {
	private String testVectorData="{\"status\":\"success\",\"data\":{\"resultType\":\"vector\",\"result\":[{\"metric\":{\"POD\":\"person-application-1.5-5dcc65c754-7ztnz\",\"application\":\"person-1.5\\t\\t\\t\\t\\t\\t\\t\\t\",\"controller\":\"*\",\"endpoint\":\"http\",\"instance\":\"10.244.4.4:19000\",\"job\":\"person-application-1-5\",\"namespace\":\"default\",\"pod\":\"person-application-1.5-5dcc65c754-7ztnz\",\"service\":\"person-application-1-5\",\"url\":\"*\"},\"value\":[1536200364.286,\"0\"]},{\"metric\":{\"POD\":\"person-application-1.5-5dcc65c754-7ztnz\",\"application\":\"person-1.5\\t\\t\\t\\t\\t\\t\\t\\t\",\"controller\":\"com.satish.monitoring.web.rest.PersonResource\",\"endpoint\":\"http\",\"instance\":\"10.244.4.4:19000\",\"job\":\"person-application-1-5\",\"namespace\":\"default\",\"pod\":\"person-application-1.5-5dcc65c754-7ztnz\",\"service\":\"person-application-1-5\",\"url\":\"[GET]/person\"},\"value\":[1536200364.286,\"0\"]},{\"metric\":{\"POD\":\"person-application-1.5-5dcc65c754-8gb82\",\"application\":\"person-1.5\\t\\t\\t\\t\\t\\t\\t\\t\",\"controller\":\"*\",\"endpoint\":\"http\",\"instance\":\"10.244.2.4:19000\",\"job\":\"person-application-1-5\",\"namespace\":\"default\",\"pod\":\"person-application-1.5-5dcc65c754-8gb82\",\"service\":\"person-application-1-5\",\"url\":\"*\"},\"value\":[1536200364.286,\"0\"]},{\"metric\":{\"POD\":\"person-application-1.5-5dcc65c754-8gb82\",\"application\":\"person-1.5\\t\\t\\t\\t\\t\\t\\t\\t\",\"controller\":\"com.satish.monitoring.web.rest.PersonResource\",\"endpoint\":\"http\",\"instance\":\"10.244.2.4:19000\",\"job\":\"person-application-1-5\",\"namespace\":\"default\",\"pod\":\"person-application-1.5-5dcc65c754-8gb82\",\"service\":\"person-application-1-5\",\"url\":\"[GET]/person\"},\"value\":[1536200364.286,\"0\"]},{\"metric\":{\"POD\":\"person-application-1.5-5dcc65c754-8xh22\",\"application\":\"person-1.5\\t\\t\\t\\t\\t\\t\\t\\t\",\"controller\":\"*\",\"endpoint\":\"http\",\"instance\":\"10.244.1.4:19000\",\"job\":\"person-application-1-5\",\"namespace\":\"default\",\"pod\":\"person-application-1.5-5dcc65c754-8xh22\",\"service\":\"person-application-1-5\",\"url\":\"*\"},\"value\":[1536200364.286,\"0\"]},{\"metric\":{\"POD\":\"person-application-1.5-5dcc65c754-8xh22\",\"application\":\"person-1.5\\t\\t\\t\\t\\t\\t\\t\\t\",\"controller\":\"com.satish.monitoring.web.rest.PersonResource\",\"endpoint\":\"http\",\"instance\":\"10.244.1.4:19000\",\"job\":\"person-application-1-5\",\"namespace\":\"default\",\"pod\":\"person-application-1.5-5dcc65c754-8xh22\",\"service\":\"person-application-1-5\",\"url\":\"[GET]/person\"},\"value\":[1536200364.286,\"0\"]}]}}";

	public void testParser() {
		DefaultQueryResult<VectorData> result = ConvertUtil.convertQueryResultString(testVectorData);
		System.out.println("-----" +result.getResult().size());
		for(Data data : result.getResult()) {
			System.out.println("=======>" + data);
		}			
	}
}
