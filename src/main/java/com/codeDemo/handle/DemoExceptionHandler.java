package com.codeDemo.handle;

import com.alibaba.fastjson.JSON;
import com.codeDemo.vo.Result;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.BindException;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;


@RestControllerAdvice
@Slf4j
public class DemoExceptionHandler {


    @ExceptionHandler(BindException.class)
    public Result paramHandler(BindException e) {
        log.error("参数异常", e);
        BindingResult result = e.getBindingResult();

        for (FieldError fieldError : result.getFieldErrors()) {
            String field = fieldError.getField();
            String defaultMessage = fieldError.getDefaultMessage();
        }


        HashMap<String, Object> resp = new HashMap<>(5);
        result.getFieldErrors().forEach(error -> resp.put(error.getField(), error.getDefaultMessage()));
        log.info("异常内容，{}", JSON.toJSONString(resp));
        return Result.error().message(JSON.toJSONString(resp));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public Result paramHandler(MethodArgumentNotValidException e) {
        log.error("方法异常", e);
        return Result.error();
    }

    @ExceptionHandler(Exception.class)
    public Result otherHandler(Exception e) {
        log.error("通用异常", e);
        return Result.error().message(e.getMessage());
    }
}
