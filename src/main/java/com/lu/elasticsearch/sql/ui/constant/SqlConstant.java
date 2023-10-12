package com.lu.elasticsearch.sql.ui.constant;

import com.facebook.presto.sql.parser.SqlParser;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SqlConstant {
    /**
     * presto sql parse
     */
    public static final SqlParser SQL_PARSER = new SqlParser();
    /**
     * all columns
     */
    public static final String ALL_COLUMNS = "*";
    public static final String INCLUDE = "include";
    public static final String EXCLUDE = "exclude";
    public static final String COUNT = "count";
    public static final Set<String> FUNC_SET = new HashSet<>(Arrays.asList(INCLUDE, EXCLUDE));
    public static final String TOP_HITS = "topHits";
}
