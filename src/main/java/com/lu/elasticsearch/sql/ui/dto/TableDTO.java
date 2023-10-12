package com.lu.elasticsearch.sql.ui.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@AllArgsConstructor
@Data
public class TableDTO {
    private String[] columns;
    private List<String[]> tableData;
}
