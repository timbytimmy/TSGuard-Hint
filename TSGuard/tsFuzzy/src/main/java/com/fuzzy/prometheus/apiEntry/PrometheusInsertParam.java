package com.fuzzy.prometheus.apiEntry;

import build.buf.gen.io.prometheus.write.v2.Request;
import build.buf.gen.io.prometheus.write.v2.Sample;
import build.buf.gen.io.prometheus.write.v2.TimeSeries;
import com.fuzzy.prometheus.apiEntry.entity.CollectorAttribute;
import com.fuzzy.prometheus.constant.PrometheusLabelConstant;
import com.google.protobuf.ByteString;
import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Data
public class PrometheusInsertParam extends PrometheusRequestParam {
    // <MetricName, Collector>
    Map<String, CollectorAttribute> collectorList;

    public PrometheusInsertParam() {
        this.collectorList = new HashMap<>();
    }

    public byte[] snappyCompressedRequest(String metricName) throws IOException {
        // 序列化为 Protobuf 字节并压缩
        ByteString data = createRequestForRemoteWrite(metricName).toByteString();
        return Snappy.compress(data.toByteArray());
    }

    public Request createRequestForRemoteWrite(String metricName) {
        Request.Builder requestBuilder = Request.newBuilder();

        // 处理时序指标
        CollectorAttribute metricVal = collectorList.get(metricName);
        Map<String, String> labels = new HashMap<>();
        labels.put(PrometheusLabelConstant.METRIC.getLabel(), metricName);
        labels.put(PrometheusLabelConstant.TABLE.getLabel(), metricVal.getTableName());
        if (!StringUtils.isBlank(metricVal.getTimeSeriesName()))
            labels.put(PrometheusLabelConstant.TIME_SERIES.getLabel(), metricVal.getTimeSeriesName());
        if (!ObjectUtils.isEmpty(metricVal.getLabels()))
            labels.putAll(metricVal.getLabels());
        // 添加标签按照指标逐条添加至 requestBuilder 符号表（严格交替存储）
        addSymbol(requestBuilder, labels);

        // 创建 TimeSeries, 相关标签索引需准确对应符号表中位置
        TimeSeries.Builder tsBuilder = TimeSeries.newBuilder();
        List<Integer> labelRefs = IntStream.range(0, labels.size() * 2)
                .boxed()
                .collect(Collectors.toList());
        tsBuilder.addAllLabelsRefs(labelRefs);

        // 生成采样点
        List<Sample> sampleList = new ArrayList<>();
        for (int i = 0; i < metricVal.getDoubleValues().size(); i++) {
            sampleList.add(Sample.newBuilder()
                    .setValue(metricVal.getDoubleValues().get(i))
                    .setTimestamp(metricVal.getTimestamps().get(i))
                    .build());
        }
        sampleList.sort(Comparator.comparingLong(Sample::getTimestamp));

        tsBuilder.addAllSamples(sampleList);
        return requestBuilder.addTimeseries(tsBuilder.build()).build();
    }

    private void addSymbol(Request.Builder requestBuilder, Map<String, String> labels) {
        labels.forEach((label, value) -> {
            requestBuilder.addSymbols(label);
            requestBuilder.addSymbols(value);
        });
    }
}
