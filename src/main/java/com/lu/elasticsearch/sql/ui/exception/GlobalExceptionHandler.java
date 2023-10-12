package com.lu.elasticsearch.sql.ui.exception;

import com.lu.elasticsearch.sql.ui.vo.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

import java.io.IOException;

@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {
    @ExceptionHandler(value = Exception.class)
    public Result<?> handlerException(Exception e) {
        log.error("未知异常: ", e);
        return Result.fail(HttpStatus.INTERNAL_SERVER_ERROR.value(), "未知异常");
    }

    @ExceptionHandler(value = IOException.class)
    public Result<?> handlerException(IOException e) {
        log.error("查询es集群异常: ", e);
        return Result.fail(HttpStatus.BAD_REQUEST.value(), "请求异常");
    }

    @ExceptionHandler(value = IllegalArgumentException.class)
    public Result<?> handlerException(IllegalArgumentException e) {
        log.error("参数异常: ", e);
        return Result.fail(HttpStatus.BAD_REQUEST.value(), e.getMessage());
    }
}
