package com.benchmark.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.*;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.Set;

@Slf4j
public class HttpClientUtils {

    public static String sendRequest(String url, String data, Map<String, String> heads, HttpRequestEnum requestType) {
        switch (requestType) {
            case GET:
                return get(url, heads);
            case POST:
                return post(url, data, heads);
            case DELETE:
                return delete(url, heads);
            case PUT:
                return put(url, data, heads);
            default:
                log.error("发送请求类型不存在");
                return null;
        }
    }

    /**
     * http get
     *
     * @param url   可带参数的 url 链接
     * @param heads http 头信息
     */
    private static String get(String url, Map<String, String> heads) {
        org.apache.http.client.HttpClient httpClient = HttpClients.createDefault();
        HttpResponse httpResponse = null;
        String result = "";
        HttpGet httpGet = new HttpGet(url);
        if (heads != null) {
            Set<String> keySet = heads.keySet();
            for (String s : keySet) {
                httpGet.addHeader(s, heads.get(s));
            }
        }

        try {
            httpResponse = httpClient.execute(httpGet);
            HttpEntity httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                result = EntityUtils.toString(httpEntity, "utf-8");

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    /**
     * http post
     */
    private static String post(String url, String data, Map<String, String> heads) {
        org.apache.http.client.HttpClient httpClient = HttpClients.createDefault();
        HttpResponse httpResponse = null;
        String result = "";

        HttpPost httpPost = new HttpPost(url);
        if (heads != null) {
            Set<String> keySet = heads.keySet();
            for (String s : keySet) {
                httpPost.addHeader(s, heads.get(s));
            }
        }
        try {
            StringEntity s = new StringEntity(data, "utf-8");
            httpPost.setEntity(s);
            httpResponse = httpClient.execute(httpPost);
            HttpEntity httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                result = EntityUtils.toString(httpEntity, "utf-8");
            }
        } catch (IOException e) {
            log.error("发送POST请求失败, e:", e);
        }
        return result;
    }

    public static String post(String url, byte[] data, Map<String, String> headers) {
        // 创建HttpClient实例
        String result = "";
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            // 创建POST请求对象
            HttpPost httpPost = new HttpPost(url);

            // 包装二进制数据为请求实体
            ByteArrayEntity entity = new ByteArrayEntity(data);

            // 设置关键请求头（根据实际需求调整）
            httpPost.setEntity(entity);

            // 可选：添加其他自定义Header
            httpPost.setHeader("Authorization", "Bearer your_token_here");
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                httpPost.setHeader(entry.getKey(), entry.getValue());
            }

            // 执行请求并获取响应
            try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
                // 处理响应状态码
                int statusCode = response.getStatusLine().getStatusCode();

                // 处理响应内容
                if (statusCode >= 200 && statusCode < 300) {
                    HttpEntity responseEntity = response.getEntity();
                    result = String.format("status: %s", statusCode);
                    if (responseEntity != null)
                        result += EntityUtils.toString(responseEntity);
                } else {
                    result = String.format("Request failed with status code: %s\n res:%s", statusCode, response);
                }
            }
        } catch (Exception e) {
            log.error("发送POST请求失败, e:", e);
        }
        return result;
    }

    public static boolean sendPointDataToDB(String url, byte[] data, Map<String, String> headers) {
        try {
            URL realUrl = new URL(url);
            URLConnection conn = realUrl.openConnection();
            headers.entrySet().forEach(header -> conn.setRequestProperty(header.getKey(), header.getValue()));
            conn.setRequestProperty("accept", "*/*");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // fill and send content
            DataOutputStream dos = new DataOutputStream(conn.getOutputStream());
            dos.write(data);
            dos.flush();
            // get response (Do not comment this line, or the data insertion will be failed)
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
//            String line;
//            while ((line = in.readLine()) != null) {
//                result += line;
//            }
//            System.out.println(result);
            return true;
        } catch (Exception e) {
            log.error("发送错误, e:", e);
            return false;
        }
    }

    /*private static String post(String url, String data, Map<String, String> heads) {
        StringBuilder sb = new StringBuilder();
        try {
            URLConnection conn = new URL(url).openConnection();

            if (heads != null) {
                Set<String> keySet = heads.keySet();
                for (String s : keySet) {
                    conn.setRequestProperty(s, heads.get(s));
                }
            }
            conn.setDoOutput(true);
            conn.setDoInput(true);
            // fill and send content
            DataOutputStream dos = new DataOutputStream(conn.getOutputStream());
            dos.write(data.getBytes());
            dos.flush();
            // get response (Do not comment this line, or the data insertion will be failed)
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String str;
            while ((str = in.readLine()) != null) {
                sb.append(str);
            }
        } catch (Exception e) {
            log.error("", e);
        }
        return sb.toString();
    }*/

    /**
     * http delete
     *
     * @param url
     * @param heads http 头信息
     */
    private static String delete(String url, Map<String, String> heads) {
        org.apache.http.client.HttpClient httpClient = HttpClients.createDefault();
        HttpResponse httpResponse = null;
        String result = "";

        HttpDelete httpDelete = new HttpDelete(url);
        if (heads != null) {
            Set<String> keySet = heads.keySet();
            for (String s : keySet) {
                httpDelete.addHeader(s, heads.get(s));
            }
        }

        try {
            httpResponse = httpClient.execute(httpDelete);
            HttpEntity httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                result = EntityUtils.toString(httpEntity, "utf-8");

            }

        } catch (IOException e) {
            e.printStackTrace();

        }
        return result;
    }

    /**
     * http put
     */
    private static String put(String url, String data, Map<String, String> heads) {
        org.apache.http.client.HttpClient httpClient = HttpClients.createDefault();
        HttpResponse httpResponse = null;
        String result = "";

        HttpPut httpPut = new HttpPut(url);
        if (heads != null) {
            Set<String> keySet = heads.keySet();
            for (String s : keySet) {
                httpPut.addHeader(s, heads.get(s));
            }
        }

        try {
            StringEntity s = new StringEntity(data, "utf-8");
            httpPut.setEntity(s);
            httpResponse = httpClient.execute(httpPut);
            HttpEntity httpEntity = httpResponse.getEntity();
            if (httpEntity != null) {
                result = EntityUtils.toString(httpEntity, "utf-8");

            }

        } catch (IOException e) {
            e.printStackTrace();

        }
        return result;
    }
}
