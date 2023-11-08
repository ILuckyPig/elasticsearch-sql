package com.lu.elasticsearch.sql.ui.service;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lu.elasticsearch.sql.ui.common.result.ResultHandlerFactory;
import com.lu.elasticsearch.sql.ui.common.result.handler.ResultHandler;
import com.lu.elasticsearch.sql.ui.common.table.Table;
import com.lu.elasticsearch.sql.ui.dao.ElasticsearchDao;
import com.lu.elasticsearch.sql.ui.dto.DslDTO;
import com.lu.elasticsearch.sql.ui.dto.TableDTO;
import com.lu.elasticsearch.sql.ui.util.JsonUtil;
import com.lu.elasticsearch.sql.ui.vo.SqlVO;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.util.EntityUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Slf4j
@Service
public class SqlEditorService {
    private final ElasticsearchDao elasticsearchDao;

    public SqlEditorService(ElasticsearchDao elasticsearchDao) {
        this.elasticsearchDao = elasticsearchDao;
    }

    public DslDTO explain(SqlVO sqlVO) throws IOException {
        String url = sqlVO.getUrl();
        String username = sqlVO.getUsername();
        String password = sqlVO.getPassword();
        String sql = sqlVO.getSql();
        try (CloseableHttpResponse response = elasticsearchDao.explain(url, username, password, sql)) {
            HttpEntity entity = response.getEntity();
            String result = EntityUtils.toString(entity);
            EntityUtils.consume(entity);
            return new DslDTO(response.getStatusLine().getStatusCode(), JsonUtil.readValue(result, ObjectNode.class));
        }
    }

    public TableDTO query(SqlVO sqlVO) throws IOException {
        String url = sqlVO.getUrl();
        String username = sqlVO.getUsername();
        String password = sqlVO.getPassword();
        String sql = sqlVO.getSql();
        try (CloseableHttpResponse response = elasticsearchDao.query(url, username, password, sql)) {
            HttpEntity entity = response.getEntity();
            String result = EntityUtils.toString(entity);
            EntityUtils.consume(entity);

            ResultHandler resultHandler = ResultHandlerFactory.createResultHandler(sql, result, false, false, false);
            Table table = resultHandler.getTable(sql, result);

            return new TableDTO(table.getBasicTableScheme().getColumns(), table.getTableData());
        }
    }
}
