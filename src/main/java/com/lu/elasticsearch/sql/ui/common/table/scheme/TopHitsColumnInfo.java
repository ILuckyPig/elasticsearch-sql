package com.lu.elasticsearch.sql.ui.common.table.scheme;

import lombok.Data;

@Data
public class TopHitsColumnInfo {
    private String[] originalColumns;
    private String topHitsName;
    private boolean includeAllColumns;
    private String[] includeColumns;
}
