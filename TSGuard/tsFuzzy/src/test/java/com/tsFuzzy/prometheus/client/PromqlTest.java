package com.tsFuzzy.prometheus.client;

import com.fuzzy.prometheus.client.converter.ConvertUtil;
import com.fuzzy.prometheus.client.converter.am.DefaultAlertManagerResult;
import com.fuzzy.prometheus.client.converter.label.DefaultLabelResult;
import com.fuzzy.prometheus.client.converter.query.DefaultQueryResult;
import com.fuzzy.prometheus.client.converter.query.MatrixData;
import com.fuzzy.prometheus.client.converter.query.QueryResultItemValue;
import com.fuzzy.prometheus.client.converter.query.VectorData;
import com.fuzzy.prometheus.client.converter.scrapeTarget.DefaultTargetResult;
import com.fuzzy.prometheus.client.converter.series.DefaultSeriesResult;
import com.fuzzy.prometheus.client.converter.series.SeriesTagKeyConstant;
import com.fuzzy.prometheus.client.converter.status.DefaultConfigResult;
import com.fuzzy.prometheus.client.builder.*;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

@Slf4j
public class PromqlTest extends TestCase {
    private final static String TARGET_SERVER = "http://111.229.183.22:9990";

    private RestTemplate template = null;

    @Override
    protected void setUp() throws Exception {
        HttpComponentsClientHttpRequestFactory httpRequestFactory = new HttpComponentsClientHttpRequestFactory();
        httpRequestFactory.setConnectTimeout(1000);
        httpRequestFactory.setReadTimeout(2000);
        HttpClient httpClient = HttpClientBuilder.create()
                .setMaxConnTotal(100)
                .setMaxConnPerRoute(10)
                .build();
        httpRequestFactory.setHttpClient(httpClient);

        template = new RestTemplate(httpRequestFactory);
    }

    private static String ConvertEpocToFormattedDate(String format, double epocTime) {
        SimpleDateFormat formatter = new SimpleDateFormat(format);
        return formatter.format(new Date(Math.round(epocTime * 1000)));
    }

    public void testSimpleRangeQuery() throws MalformedURLException {
        RangeQueryBuilder rangeQueryBuilder = QueryBuilderType.RangeQuery.newInstance(TARGET_SERVER);
        URI targetUri = rangeQueryBuilder.withQuery("100 - avg(rate(node_cpu{application=\"node_exporter\", mode=\"idle\"}[1m])) by (instance)*100")
                .withStartEpochTime(System.currentTimeMillis() / 1000 - 60 * 10)
                .withEndEpochTime(System.currentTimeMillis() / 1000)
                .withStepTime("60s")
                .build();

        System.out.println(targetUri.toURL().toString());

        String rtVal = template.getForObject(targetUri, String.class);


        DefaultQueryResult<MatrixData> result = ConvertUtil.convertQueryResultString(rtVal);

        for (MatrixData matrixData : result.getResult()) {
            System.out.println(String.format("%s", matrixData.getMetric().get("instance")));
            for (QueryResultItemValue itemValue : matrixData.getDataValues()) {
                System.out.println(String.format("%s %10.2f ",
                        ConvertEpocToFormattedDate("yyyy-MM-dd hh:mm:ss", itemValue.getTimestamp()),
                        itemValue.getValue()
                ));
            }
        }

    }

    public void testSimpleVectorQuery() throws MalformedURLException {
        InstantQueryBuilder iqb = QueryBuilderType.InstantQuery.newInstance(TARGET_SERVER);
        URI targetUri = iqb.withQuery("node_cpu{application=\"node_exporter\", mode=\"idle\"}[1m]").build();


        String rtVal = template.getForObject(targetUri, String.class);


        DefaultQueryResult<MatrixData> result = ConvertUtil.convertQueryResultString(rtVal);


        for (MatrixData matrixData : result.getResult()) {
            System.out.println(String.format("%s", matrixData.getMetric().get("instance")));
            for (QueryResultItemValue itemValue : matrixData.getDataValues()) {
                System.out.println(String.format("%s %10.2f ",
                        ConvertEpocToFormattedDate("yyyy-MM-dd hh:mm:ss", itemValue.getTimestamp()),
                        itemValue.getValue()
                ));
            }
        }

        System.out.println(targetUri.toURL().toString());
//		System.out.println(result);		
    }

    public void testSimpleInstantQuery() throws MalformedURLException {
        InstantQueryBuilder iqb = QueryBuilderType.InstantQuery.newInstance(TARGET_SERVER);
        URI targetUri = iqb.withQuery("100 - avg(rate(node_cpu{application=\"node_exporter\", mode=\"idle\"}[1m])) by (instance)*100").build();
        System.out.println(targetUri.toURL().toString());


        String rtVal = template.getForObject(targetUri, String.class);


        DefaultQueryResult<VectorData> result = ConvertUtil.convertQueryResultString(rtVal);


        for (VectorData vectorData : result.getResult()) {
            System.out.println(String.format("%s %s %10.2f",
                    vectorData.getMetric().get("instance"),
                    vectorData.getFormattedTimestamps("yyyy-MM-dd hh:mm:ss"),
                    vectorData.getValue()));
        }

        System.out.println(result);
    }

    public void testSimpleLabel() throws MalformedURLException {
        LabelMetaQueryBuilder lmqb = QueryBuilderType.LabelMetadaQuery.newInstance(TARGET_SERVER);
        URI targetUri = lmqb.withLabel("database").build();
        System.out.println(targetUri.toURL().toString());


        String rtVal = template.getForObject(targetUri, String.class);


        DefaultLabelResult result = ConvertUtil.convertLabelResultString(rtVal);


        System.out.println(result);
    }

    public void testSimpleConfig() throws MalformedURLException {
        StatusMetaQueryBuilder smqb = QueryBuilderType.StatusMetadaQuery.newInstance(TARGET_SERVER);
        URI targetUri = smqb.build();
        System.out.println(targetUri.toURL().toString());


        String rtVal = template.getForObject(targetUri, String.class);


        DefaultConfigResult result = ConvertUtil.convertConfigResultString(rtVal);


        System.out.println(result);
    }

    public void testSimpleTargets() throws MalformedURLException {
        TargetMetaQueryBuilder tmqb = QueryBuilderType.TargetMetadaQuery.newInstance(TARGET_SERVER);
        URI targetUri = tmqb.build();
        System.out.println(targetUri.toURL().toString());


        String rtVal = template.getForObject(targetUri, String.class);


        DefaultTargetResult result = ConvertUtil.convertTargetResultString(rtVal);


        log.info("{}", result);
    }

    public void testSimpleSeries() throws MalformedURLException {
        // TODO 该方法找到的序列包含已经被执行删除的序列？
        SeriesMetaQueryBuilder smqb = QueryBuilderType.SeriesMetadaQuery.newInstance("http://111.229.183.22:9990");
        URI targetUri = smqb.withSelector("match[]={database=\"pqsdb0\"}").build();
        System.out.println(targetUri.toURL().toString());
        String rtVal = template.getForObject(targetUri, String.class);
        DefaultSeriesResult seriesResult = ConvertUtil.convertSeriesResultString(rtVal);
        log.info("{}", seriesResult);
        log.info("{}", seriesResult.getResult().get(0).get(SeriesTagKeyConstant.__NAME__.getKey()));
    }

    public void testDeleteSeries() throws MalformedURLException {
        SeriesDeleteBuilder smqb = QueryBuilderType.SeriesDelete.newInstance("http://111.229.183.22:9990");
        URI targetUri = smqb.withSelector("match[]={__name__=\"vector2\"}").build();
        System.out.println(targetUri.toURL().toString());
        String rtVal = template.postForObject(targetUri, null, String.class);
        log.info("{}", rtVal);
    }

    public void testDeletePushGateway() throws MalformedURLException {
        template.delete(URI.create("http://111.229.183.22:9091/metrics/job/pqsdb0_t1_c0"));
    }

    public void testCleanTombstones() throws MalformedURLException {
        CleanTombstonesBuilder builder = QueryBuilderType.CleanTombstones.newInstance("http://111.229.183.22:9990");
        URI targetUri = builder.build();
        System.out.println(targetUri.toURL().toString());
        String rtVal = template.postForObject(targetUri, null, String.class);
        log.info("{}", rtVal);
    }

    public void testSimpleAlertManager() throws MalformedURLException {
        AlertManagerMetaQueryBuilder ammqb = QueryBuilderType.AlertManagerMetadaQuery.newInstance(TARGET_SERVER);
        URI targetUri = ammqb.build();
        System.out.println(targetUri.toURL().toString());


        String rtVal = template.getForObject(targetUri, String.class);


        DefaultAlertManagerResult result = ConvertUtil.convertAlertManagerResultString(rtVal);


        System.out.println(result);
    }

    public void testWriteDataToPrometheus() throws IOException, InterruptedException {
        PushGateway pushGateway = new PushGateway("111.229.183.22:9091");
        Gauge gauge = Gauge.build()
                .name("vector1")
                .labelNames("database", "table")
                .help("vector1")
                .register();
        Gauge gauge2 = Gauge.build()
                .name("vector2")
                .labelNames("database", "table")
                .help("vector2")
                .register();

//        Gauge counter2 = Gauge.build()
//                .name("column2")
//                .labelNames("database", "table")
//                .help("column2")
//                .register();
        while (true) {
            gauge.labels("database0", "t0").set(0.6);
            gauge.labels("database1", "t0").set(0.7);
            gauge.labels("database2", "t0").set(0.8);

            gauge2.labels("database0", "t0").set(1.6);
            gauge2.labels("database2", "t0").set(1.7);
            gauge2.labels("database3", "t0").set(1.8);

            pushGateway.push(gauge, "vector1");
            pushGateway.push(gauge2, "vector2");
            log.info("write success");
            Thread.sleep(15000);
        }
    }
}
