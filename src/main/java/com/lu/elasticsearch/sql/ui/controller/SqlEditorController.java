package com.lu.elasticsearch.sql.ui.controller;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.lu.elasticsearch.sql.ui.dto.DslDTO;
import com.lu.elasticsearch.sql.ui.dto.TableDTO;
import com.lu.elasticsearch.sql.ui.service.SqlEditorService;
import com.lu.elasticsearch.sql.ui.util.JsonUtil;
import com.lu.elasticsearch.sql.ui.vo.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestController
@RequestMapping("/editor")
public class SqlEditorController {
    private final SqlEditorService sqlEditorService;

    public SqlEditorController(SqlEditorService sqlEditorService) {
        this.sqlEditorService = sqlEditorService;
    }

    @RequestMapping(value = "/explain", method = RequestMethod.POST)
    public @ResponseBody Result<DslVO> explain(@RequestBody SqlVO sqlVO) throws IOException {
        log.info("explain SqlVO is {}", sqlVO);
        DslDTO dslDTO = sqlEditorService.explain(sqlVO);
        DslVO dslVO = new DslVO();
        BeanUtils.copyProperties(dslDTO, dslVO);
        return Result.data(dslVO);
    }

    @RequestMapping(value = "/query", method = RequestMethod.POST)
    public @ResponseBody Result<TableVO> query(@RequestBody SqlVO sqlVO) throws IOException {
        log.info("query SqlVO is {}", sqlVO);
        TableDTO tableDTO = sqlEditorService.query(sqlVO);
        TableVO tableVO = convertToTableVO(tableDTO);
        return Result.data(tableVO);
    }

    private TableVO convertToTableVO(TableDTO tableDTO) {
        String[] columns = tableDTO.getColumns();
        TableColumnVO[] tableColumns = new TableColumnVO[columns.length];
        for (int i = 0; i < columns.length; i++) {
            String column = columns[i];
            tableColumns[i] = new TableColumnVO("f" + i, column);
        }

        List<ObjectNode> tableData = tableDTO.getTableData()
                .stream()
                .map(rows -> {
                    ObjectNode objectNode = JsonUtil.createObjectNode();
                    for (int i = 0; i < rows.length; i++) {
                        objectNode.put("f" + i, rows[i]);
                    }
                    return objectNode;
                })
                .collect(Collectors.toList());

        return new TableVO(tableColumns, tableData);
    }
}
