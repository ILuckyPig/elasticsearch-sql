package com.lu.elasticsearch.sql.ui.common.result.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.lu.elasticsearch.sql.ui.common.table.Table;

public interface ResultHandler {
    Table getTable(String sql, String data) throws JsonProcessingException;
}
