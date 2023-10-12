package com.lu.elasticsearch.sql.ui.common.table.scheme;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class TableScheme extends BasicTableScheme {
    private boolean refresh;
}
