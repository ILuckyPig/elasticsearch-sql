package com.lu.elasticsearch.sql.ui.common.table.scheme;

import lombok.Data;

@Data
public class BasicTableScheme {
    private String[] columns;
    private String[] groupByColumns;
}
