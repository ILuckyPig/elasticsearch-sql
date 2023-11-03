package com.lu.elasticsearch.sql.ui.dao;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.springframework.stereotype.Component;
import org.springframework.util.Base64Utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

@Component
public class ElasticsearchDao {
    private final CloseableHttpClient closeableHttpClient;
    private static final String EXPLAIN = "_sql/_explain";
    private static final String SQL = "_sql";
    private static final String JSON = "application/json";

    public ElasticsearchDao(CloseableHttpClient closeableHttpClient) {
        this.closeableHttpClient = closeableHttpClient;
    }

    public CloseableHttpResponse explain(String url, String username, String password, String sql) throws IOException {
        String uri = url + EXPLAIN;
        HttpPost httpPost = getHttpPost(uri, username, password, sql);
        return closeableHttpClient.execute(httpPost);
    }

    public CloseableHttpResponse query(String url, String username, String password, String sql)
            throws IOException {
        String uri = url + SQL;
        HttpPost httpPost = getHttpPost(uri, username, password, sql);
        return closeableHttpClient.execute(httpPost);
    }

    private HttpPost getHttpPost(String uri, String username, String password, String sql) throws UnsupportedEncodingException {
        HttpPost httpPost = new HttpPost(uri);
        if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
            String auth = username + ":" + password;
            byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(StandardCharsets.ISO_8859_1));
            httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Basic " + new String(encodedAuth));
        }
        httpPost.setHeader(HttpHeaders.CONTENT_TYPE, JSON);
        httpPost.setEntity(new StringEntity(sql));
        return httpPost;
    }
}
