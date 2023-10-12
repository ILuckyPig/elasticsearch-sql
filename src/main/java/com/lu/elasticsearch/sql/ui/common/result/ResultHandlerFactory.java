package com.lu.elasticsearch.sql.ui.common.result;


import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.tree.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.lu.elasticsearch.sql.ui.common.result.handler.*;
import com.lu.elasticsearch.sql.ui.util.JsonUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.lu.elasticsearch.sql.ui.constant.SqlConstant.SQL_PARSER;
import static com.lu.elasticsearch.sql.ui.constant.SqlConstant.TOP_HITS;

@Slf4j
public class ResultHandlerFactory {
    public static ResultHandler createResultHandler(String sql, String data, boolean showId, boolean showType, boolean showScore) throws JsonProcessingException {
        JsonNode jsonNode = JsonUtil.readValue(data, JsonNode.class);
        if (isSearch(jsonNode)) {
            if (isAggregation(jsonNode)) {
                if (isTopHits(sql)) {
                    return new TopHitsQueryResultHandler();
                }
                return new AggregationQueryResultHandler();
            }
            return new DefaultQueryResultHandler();
        }
        return new ShowQueryResultHandler();
    }

    private static boolean isSearch(JsonNode jsonNode) {
        return jsonNode.hasNonNull("hits") && jsonNode.get("hits").hasNonNull("hits");
    }

    private static boolean isTopHits(String sql) {
        Query query = (Query) SQL_PARSER.createStatement(sql, ParsingOptions.builder().build());
        QueryBody queryBody = query.getQueryBody();
        if (queryBody instanceof QuerySpecification) {
            QuerySpecification querySpecification = (QuerySpecification) queryBody;
            List<SelectItem> selectItems = querySpecification.getSelect().getSelectItems();
            for (SelectItem selectItem : selectItems) {
                if (selectItem instanceof SingleColumn) {
                    SingleColumn item = (SingleColumn) selectItem;
                    if (item.getExpression() instanceof FunctionCall) {
                        FunctionCall functionCall = (FunctionCall) item.getExpression();
                        String functionCallName = functionCall.getName().getOriginalParts().get(0);
                        if (TOP_HITS.equals(functionCallName)) {
                            return true;
                        }
                    }
                }
            }
        } else {
            log.error("unSupport query type, sql: {}", sql);
        }
        return false;
    }

    private static boolean isAggregation(JsonNode jsonNode) {
        return jsonNode.hasNonNull("aggregations");
    }
}
