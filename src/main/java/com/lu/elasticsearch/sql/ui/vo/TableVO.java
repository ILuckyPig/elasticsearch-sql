package com.lu.elasticsearch.sql.ui.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableVO {
    private String[] columns;
    private List<String[]> tableData;
}
