package com.lu.elasticsearch.sql.ui.vo;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class TableVO {
    private TableColumnVO[] columns;
    private List<ObjectNode> tableData;
}
