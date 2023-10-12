package com.lu.elasticsearch.sql.ui.service;

import com.lu.elasticsearch.sql.ui.dto.DslDTO;
import com.lu.elasticsearch.sql.ui.dto.TableDTO;
import com.lu.elasticsearch.sql.ui.util.JsonUtil;
import com.lu.elasticsearch.sql.ui.vo.SqlVO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SqlEditorServiceTestCase {
    @Autowired
    private SqlEditorService sqlEditorService;

    @Test
    public void testExplain() throws IOException {
        SqlVO sqlVO = new SqlVO();
        sqlVO.setUrl("http://localhost:9200/");
        // sqlVO.setUsername("username");
        // sqlVO.setPassword("password");
        sqlVO.setSql("select * from test_nested_index2 group by maoyanId limit 1");
        DslDTO dslDTO = sqlEditorService.explain(sqlVO);
        System.out.println(JsonUtil.toJsonString(dslDTO));
    }

    @Test
    public void testQuery() throws IOException {
        SqlVO sqlVO = new SqlVO();
        sqlVO.setUrl("http://localhost:9200/");
        // sqlVO.setUsername("username");
        // sqlVO.setPassword("password");
        sqlVO.setSql("SELECT market,count(*) c,topHits(size=1,box='desc',include='date,market,movieSource,box,id',exclude='id') as topHits,count(distinct maoyanId) from test_nested_index2 group by market");
        TableDTO tableDTO = sqlEditorService.query(sqlVO);
        System.out.println(JsonUtil.toJsonString(tableDTO));
    }
}
