package com.lu.elasticsearch.sql.ui.dao;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticsearchDaoTestCase {
    @Autowired
    private ElasticsearchDao elasticsearchDao;

    @Test
    public void testExplain() throws IOException {
        String url = "https://localhost:9200/";
        String username = null;
        String password = null;
        String sql = "select * from test_nested_index2 group by box limit 1";
        CloseableHttpResponse response = elasticsearchDao.explain(url, username, password, sql);
        HttpEntity entity = response.getEntity();
        String string = EntityUtils.toString(entity);
        EntityUtils.consume(entity);
        System.out.println(string);
        Assert.assertNotNull(string);
    }
}
