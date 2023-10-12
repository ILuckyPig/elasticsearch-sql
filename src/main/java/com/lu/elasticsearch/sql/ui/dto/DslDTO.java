package com.lu.elasticsearch.sql.ui.dto;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DslDTO {
    private Integer status;
    private ObjectNode body;
}
