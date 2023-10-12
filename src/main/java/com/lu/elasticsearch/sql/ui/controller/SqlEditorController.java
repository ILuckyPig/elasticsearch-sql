package com.lu.elasticsearch.sql.ui.controller;

import com.lu.elasticsearch.sql.ui.dto.DslDTO;
import com.lu.elasticsearch.sql.ui.dto.TableDTO;
import com.lu.elasticsearch.sql.ui.service.SqlEditorService;
import com.lu.elasticsearch.sql.ui.vo.DslVO;
import com.lu.elasticsearch.sql.ui.vo.Result;
import com.lu.elasticsearch.sql.ui.vo.SqlVO;
import com.lu.elasticsearch.sql.ui.vo.TableVO;
import org.springframework.beans.BeanUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;

@RestController
@RequestMapping("/editor")
public class SqlEditorController {
    private final SqlEditorService sqlEditorService;

    public SqlEditorController(SqlEditorService sqlEditorService) {
        this.sqlEditorService = sqlEditorService;
    }

    @RequestMapping(value = "/explain", method = RequestMethod.POST)
    public @ResponseBody Result<DslVO> explain(@RequestBody SqlVO sqlVO) throws IOException {
        DslDTO dslDTO = sqlEditorService.explain(sqlVO);
        DslVO dslVO = new DslVO();
        BeanUtils.copyProperties(dslDTO, dslVO);
        return Result.data(dslVO);
    }

    @RequestMapping(value = "/query", method = RequestMethod.POST)
    public @ResponseBody Result<TableVO> query(@RequestBody SqlVO sqlVO) throws IOException {
        TableDTO tableDTO = sqlEditorService.query(sqlVO);
        TableVO tableVO = new TableVO();
        BeanUtils.copyProperties(tableDTO, tableVO);
        return Result.data(tableVO);
    }
}
