package com.lu.elasticsearch.sql.ui.common.table;

import com.lu.elasticsearch.sql.ui.common.table.scheme.BasicTableScheme;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class Table {
    private BasicTableScheme basicTableScheme;
    private List<String[]> tableData;
}
