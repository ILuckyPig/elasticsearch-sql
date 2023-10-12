package com.lu.elasticsearch.sql.ui.common;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.lu.elasticsearch.sql.ui.common.result.handler.AggregationQueryResultHandler;
import com.lu.elasticsearch.sql.ui.common.result.handler.DefaultQueryResultHandler;
import com.lu.elasticsearch.sql.ui.common.result.handler.TopHitsQueryResultHandler;
import com.lu.elasticsearch.sql.ui.common.table.Table;
import com.lu.elasticsearch.sql.ui.util.JsonUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ResultHandlerTestCase {
    private static DefaultQueryResultHandler defaultQueryResultHandler;
    private static AggregationQueryResultHandler aggregationQueryResultHandler;
    private static TopHitsQueryResultHandler topHitsQueryResultHandler;

    @BeforeClass
    public static void beforeClass() throws Exception {
        defaultQueryResultHandler = new DefaultQueryResultHandler();
        aggregationQueryResultHandler = new AggregationQueryResultHandler();
        topHitsQueryResultHandler = new TopHitsQueryResultHandler();
    }

    @Test
    public void testDefaultQuery() throws JsonProcessingException {
        String sql = "SELECT date,market,source FROM test_nested_index2\n";
        String json = "{ \"took\": 1, \"timed_out\": false, \"_shards\": { \"total\": 5, \"successful\": 5, \"skipped\": 0, \"failed\": 0 }, \"hits\": { \"total\": 5, \"max_score\": 1.0, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220914_123_hk\", \"_score\": 1.0, \"_source\": { \"date\": \"20220914\", \"market\": \"hk\", \"source\": [ { \"location\": \"hk\" }, { \"location\": \"mainland\" } ] } } ] } }";
        Table table = defaultQueryResultHandler.getTable(sql, json);
        System.out.println(JsonUtil.toJsonString(table));
    }

    @Test
    public void testAliasQuery() throws JsonProcessingException {
        String sql = "SELECT date as dt,market,source FROM test_nested_index2";
        String json = "{ \"took\": 7, \"timed_out\": false, \"_shards\": { \"total\": 5, \"successful\": 5, \"skipped\": 0, \"failed\": 0 }, \"hits\": { \"total\": 5, \"max_score\": 1.0, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220914_123_hk\", \"_score\": 1.0, \"_source\": { \"market\": \"hk\", \"source\": [ { \"location\": \"hk\" }, { \"location\": \"mainland\" } ] }, \"fields\": { \"dt\": [ \"20220914\" ] } } ] } }";
        Table table = defaultQueryResultHandler.getTable(sql, json);
        System.out.println(JsonUtil.toJsonString(table));
    }

    @Test
    public void testAllQuery() throws JsonProcessingException {
        String sql = "SELECT * FROM test_nested_index2";
        String json = "{ \"took\": 1, \"timed_out\": false, \"_shards\": { \"total\": 5, \"successful\": 5, \"skipped\": 0, \"failed\": 0 }, \"hits\": { \"total\": 5, \"max_score\": 1.0, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220914_123_hk\", \"_score\": 1.0, \"_source\": { \"date\": \"20220914\", \"market\": \"hk\", \"source\": [ { \"location\": \"hk\" }, { \"location\": \"mainland\" } ], \"box\": 2.0, \"id\": 123 } } ] } }";
        Table table = defaultQueryResultHandler.getTable(sql, json);
        System.out.println(JsonUtil.toJsonString(table));
    }

    @Test
    public void testIncludeQuery() throws JsonProcessingException {
        String sql = "SELECT date,include('box') FROM test_nested_index2";
        String json = "{ \"took\": 1, \"timed_out\": false, \"_shards\": { \"total\": 5, \"successful\": 5, \"skipped\": 0, \"failed\": 0 }, \"hits\": { \"total\": 5, \"max_score\": 1.0, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220914_123_hk\", \"_score\": 1.0, \"_source\": { \"date\": \"20220914\", \"box\": 2.0 } } ] } }";
        Table table = defaultQueryResultHandler.getTable(sql, json);
        System.out.println(JsonUtil.toJsonString(table));
    }

    @Test
    public void testExcludeQuery() throws JsonProcessingException {
        String sql = "SELECT exclude('box') FROM test_nested_index2\n";
        String json = "{ \"took\": 1, \"timed_out\": false, \"_shards\": { \"total\": 5, \"successful\": 5, \"skipped\": 0, \"failed\": 0 }, \"hits\": { \"total\": 5, \"max_score\": 1.0, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220914_123_hk\", \"_score\": 1.0, \"_source\": { \"date\": \"20220914\", \"market\": \"hk\", \"source\": [ { \"location\": \"hk\" }, { \"location\": \"mainland\" } ], \"id\": 123 } } ] } }";
        Table table = defaultQueryResultHandler.getTable(sql, json);
        System.out.println(JsonUtil.toJsonString(table));
    }

    @Test
    public void testAggregateWithoutGroupByQuery() throws JsonProcessingException {
        String sql = "SELECT avg(box) avg_box,count(*),max(box),min(box) min_box,sum(box) sum_box FROM test_nested_index2\n";
        String json = "{ \"took\": 1, \"timed_out\": false, \"_shards\": { \"total\": 5, \"successful\": 5, \"skipped\": 0, \"failed\": 0 }, \"hits\": { \"total\": 5, \"max_score\": 0.0, \"hits\": [] }, \"aggregations\": { \"min_box\": { \"value\": 1.0 }, \"COUNT(*)\": { \"value\": 5 }, \"sum_box\": { \"value\": 16.0 }, \"MAX(box)\": { \"value\": 10.0 }, \"avg_box\": { \"value\": 3.2 } } }";
        Table table = aggregationQueryResultHandler.getTable(sql, json);
        System.out.println(JsonUtil.toJsonString(table));
    }

    @Test
    public void testAggregateWithGroupByQuery() throws JsonProcessingException {
        String sql = "SELECT date,market,id,count(*),max(box) ma,min(box) mi,sum(box) s  FROM test_nested_index2 GROUP BY date,market,id\n";
        String json = "{ \"took\": 1, \"timed_out\": false, \"_shards\": { \"total\": 5, \"successful\": 5, \"skipped\": 0, \"failed\": 0 }, \"hits\": { \"total\": 5, \"max_score\": 0.0, \"hits\": [] }, \"aggregations\": { \"date\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"20220914\", \"doc_count\": 3, \"market\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"hk\", \"doc_count\": 1, \"id\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"123\", \"doc_count\": 1, \"COUNT(*)\": { \"value\": 1 }, \"s\": { \"value\": 2.0 }, \"ma\": { \"value\": 2.0 }, \"mi\": { \"value\": 2.0 } } ] } }, { \"key\": \"uk\", \"doc_count\": 1, \"id\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"999\", \"doc_count\": 1, \"COUNT(*)\": { \"value\": 1 }, \"s\": { \"value\": 10.0 }, \"ma\": { \"value\": 10.0 }, \"mi\": { \"value\": 10.0 } } ] } }, { \"key\": \"usa\", \"doc_count\": 1, \"id\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"123\", \"doc_count\": 1, \"COUNT(*)\": { \"value\": 1 }, \"s\": { \"value\": 2.0 }, \"ma\": { \"value\": 2.0 }, \"mi\": { \"value\": 2.0 } } ] } } ] } }, { \"key\": \"20220915\", \"doc_count\": 2, \"market\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"hk\", \"doc_count\": 1, \"id\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"123\", \"doc_count\": 1, \"COUNT(*)\": { \"value\": 1 }, \"s\": { \"value\": 1.0 }, \"ma\": { \"value\": 1.0 }, \"mi\": { \"value\": 1.0 } } ] } }, { \"key\": \"usa\", \"doc_count\": 1, \"id\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"123\", \"doc_count\": 1, \"COUNT(*)\": { \"value\": 1 }, \"s\": { \"value\": 1.0 }, \"ma\": { \"value\": 1.0 }, \"mi\": { \"value\": 1.0 } } ] } } ] } } ] } } }";
        Table table = aggregationQueryResultHandler.getTable(sql, json);
        System.out.println(JsonUtil.toJsonString(table));
    }

    @Test
    public void testCountDistinct() throws JsonProcessingException {
        String sql = "SELECT date,count(distinct id) FROM test_nested_index2 GROUP BY date\n";
        String json = "{ \"took\": 1, \"timed_out\": false, \"_shards\": { \"total\": 5, \"successful\": 5, \"skipped\": 0, \"failed\": 0 }, \"hits\": { \"total\": 5, \"max_score\": 0.0, \"hits\": [] }, \"aggregations\": { \"date\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"20220914\", \"doc_count\": 3, \"COUNT(DISTINCT id)\": { \"value\": 2 } }, { \"key\": \"20220915\", \"doc_count\": 2, \"COUNT(DISTINCT id)\": { \"value\": 1 } } ] } } }";
        Table table = aggregationQueryResultHandler.getTable(sql, json);
        System.out.println(JsonUtil.toJsonString(table));

        String sql1 = "SELECT count(distinct box)  FROM test_nested_index2";
        String json1 = "{ \"took\": 1, \"timed_out\": false, \"_shards\": { \"total\": 5, \"successful\": 5, \"skipped\": 0, \"failed\": 0 }, \"hits\": { \"total\": 5, \"max_score\": 0.0, \"hits\": [] }, \"aggregations\": { \"COUNT(DISTINCT box)\": { \"value\": 3 } } }";
        Table table1 = aggregationQueryResultHandler.getTable(sql1, json1);
        System.out.println(JsonUtil.toJsonString(table1));
    }

    @Test
    public void testTopHits() throws JsonProcessingException {
        String sql = "SELECT date,market,count(*) c,topHits(size=1,box='desc',include='date,market,movieSource,box,id',exclude='id') as topHits,count(distinct id) from test_nested_index2 group by date,market\n";
        String json = "{ \"took\": 1, \"timed_out\": false, \"_shards\": { \"total\": 5, \"successful\": 5, \"skipped\": 0, \"failed\": 0 }, \"hits\": { \"total\": 5, \"max_score\": 0.0, \"hits\": [] }, \"aggregations\": { \"date\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"20220914\", \"doc_count\": 3, \"market\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"hk\", \"doc_count\": 1, \"COUNT(DISTINCT id)\": { \"value\": 1 }, \"c\": { \"value\": 1 }, \"topHits\": { \"hits\": { \"total\": 1, \"max_score\": null, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220914_123_hk\", \"_score\": null, \"_source\": { \"date\": \"20220914\", \"market\": \"hk\", \"movieSource\": [ { \"location\": \"hk\" }, { \"location\": \"mainland\" } ], \"box\": 2.0 }, \"sort\": [ 2.0 ] } ] } } }, { \"key\": \"uk\", \"doc_count\": 1, \"COUNT(DISTINCT id)\": { \"value\": 1 }, \"c\": { \"value\": 1 }, \"topHits\": { \"hits\": { \"total\": 1, \"max_score\": null, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220914_999_uk\", \"_score\": null, \"_source\": { \"date\": \"20220914\", \"market\": \"uk\", \"movieSource\": [ { \"location\": \"uk\" } ], \"box\": 10.0 }, \"sort\": [ 10.0 ] } ] } } }, { \"key\": \"usa\", \"doc_count\": 1, \"COUNT(DISTINCT id)\": { \"value\": 1 }, \"c\": { \"value\": 1 }, \"topHits\": { \"hits\": { \"total\": 1, \"max_score\": null, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220914_123_usa\", \"_score\": null, \"_source\": { \"date\": \"20220914\", \"market\": \"usa\", \"movieSource\": [ { \"location\": \"hk\" }, { \"location\": \"mainland\" } ], \"box\": 2.0 }, \"sort\": [ 2.0 ] } ] } } } ] } }, { \"key\": \"20220915\", \"doc_count\": 2, \"market\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"hk\", \"doc_count\": 1, \"COUNT(DISTINCT id)\": { \"value\": 1 }, \"c\": { \"value\": 1 }, \"topHits\": { \"hits\": { \"total\": 1, \"max_score\": null, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220915_123_hk\", \"_score\": null, \"_source\": { \"date\": \"20220915\", \"market\": \"hk\", \"movieSource\": [ { \"location\": \"hk\" }, { \"location\": \"mainland\" } ], \"box\": 1.0 }, \"sort\": [ 1.0 ] } ] } } }, { \"key\": \"usa\", \"doc_count\": 1, \"COUNT(DISTINCT id)\": { \"value\": 1 }, \"c\": { \"value\": 1 }, \"topHits\": { \"hits\": { \"total\": 1, \"max_score\": null, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220915_123_usa\", \"_score\": null, \"_source\": { \"date\": \"20220915\", \"market\": \"usa\", \"movieSource\": [ { \"location\": \"hk\" }, { \"location\": \"mainland\" } ], \"box\": 1.0 }, \"sort\": [ 1.0 ] } ] } } } ] } } ] } } }";
        Table table = topHitsQueryResultHandler.getTable(sql, json);
        System.out.println(JsonUtil.toJsonString(table));

        String sql1 = "SELECT date,market,topHits(size=1,box='desc',include='*') FROM test_nested_index2 GROUP BY date,market";
        String json1 = "{ \"took\": 1, \"timed_out\": false, \"_shards\": { \"total\": 5, \"successful\": 5, \"skipped\": 0, \"failed\": 0 }, \"hits\": { \"total\": 5, \"max_score\": 0.0, \"hits\": [] }, \"aggregations\": { \"date\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"20220914\", \"doc_count\": 3, \"market\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"hk\", \"doc_count\": 1, \"topHits(size=1,box=desc,include=*)\": { \"hits\": { \"total\": 1, \"max_score\": null, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220914_123_hk\", \"_score\": null, \"_source\": { \"date\": \"20220914\", \"market\": \"hk\", \"source\": [ { \"location\": \"hk\" }, { \"location\": \"mainland\" } ], \"box\": 2.0, \"id\": 123 }, \"sort\": [ 2.0 ] } ] } } }, { \"key\": \"uk\", \"doc_count\": 1, \"topHits(size=1,box=desc,include=*)\": { \"hits\": { \"total\": 1, \"max_score\": null, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220914_999_uk\", \"_score\": null, \"_source\": { \"date\": \"20220914\", \"market\": \"uk\", \"source\": [ { \"location\": \"uk\" } ], \"box\": 10.0, \"id\": 999 }, \"sort\": [ 10.0 ] } ] } } }, { \"key\": \"usa\", \"doc_count\": 1, \"topHits(size=1,box=desc,include=*)\": { \"hits\": { \"total\": 1, \"max_score\": null, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220914_123_usa\", \"_score\": null, \"_source\": { \"date\": \"20220914\", \"market\": \"usa\", \"source\": [ { \"location\": \"hk\" }, { \"location\": \"mainland\" } ], \"box\": 2.0, \"id\": 123 }, \"sort\": [ 2.0 ] } ] } } } ] } }, { \"key\": \"20220915\", \"doc_count\": 2, \"market\": { \"doc_count_error_upper_bound\": 0, \"sum_other_doc_count\": 0, \"buckets\": [ { \"key\": \"hk\", \"doc_count\": 1, \"topHits(size=1,box=desc,include=*)\": { \"hits\": { \"total\": 1, \"max_score\": null, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220915_123_hk\", \"_score\": null, \"_source\": { \"date\": \"20220915\", \"market\": \"hk\", \"source\": [ { \"location\": \"hk\" }, { \"location\": \"mainland\" } ], \"box\": 1.0, \"id\": 123 }, \"sort\": [ 1.0 ] } ] } } }, { \"key\": \"usa\", \"doc_count\": 1, \"topHits(size=1,box=desc,include=*)\": { \"hits\": { \"total\": 1, \"max_score\": null, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220915_123_usa\", \"_score\": null, \"_source\": { \"date\": \"20220915\", \"market\": \"usa\", \"source\": [ { \"location\": \"hk\" }, { \"location\": \"mainland\" } ], \"box\": 1.0, \"id\": 123 }, \"sort\": [ 1.0 ] } ] } } } ] } } ] } } }";
        Table table1 = topHitsQueryResultHandler.getTable(sql1, json1);
        System.out.println(JsonUtil.toJsonString(table1));

        String sql2 = "SELECT count(*) c,count(distinct maoyanId),topHits(size=1,box='desc',include='date,market,movieSource,box,maoyanId',exclude='maoyanId') as topHits from test_nested_index2";
        String json2 = "{ \"took\": 1, \"timed_out\": false, \"_shards\": { \"total\": 5, \"successful\": 5, \"skipped\": 0, \"failed\": 0 }, \"hits\": { \"total\": 5, \"max_score\": 0.0, \"hits\": [] }, \"aggregations\": { \"COUNT(DISTINCT maoyanId)\": { \"value\": 2 }, \"c\": { \"value\": 5 }, \"topHits\": { \"hits\": { \"total\": 5, \"max_score\": null, \"hits\": [ { \"_index\": \"test_nested_index2\", \"_type\": \"test_nested_index\", \"_id\": \"20220914_999_uk\", \"_score\": null, \"_source\": { \"date\": \"20220914\", \"market\": \"uk\", \"movieSource\": [ { \"location\": \"uk\" } ], \"box\": 10.0 }, \"sort\": [ 10.0 ] } ] } } } }";
        Table table2 = topHitsQueryResultHandler.getTable(sql2, json2);
        System.out.println(JsonUtil.toJsonString(table2));
    }
}
