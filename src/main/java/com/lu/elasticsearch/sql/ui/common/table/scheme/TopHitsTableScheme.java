package com.lu.elasticsearch.sql.ui.common.table.scheme;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class TopHitsTableScheme extends BasicTableScheme {
    private String topHitsName;
    private Integer topHitsColumnStartIndex;
    private Integer topHitsColumnEndIndex;
}
