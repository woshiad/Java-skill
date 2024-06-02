package com.codeDemo.controller;

import com.alibaba.fastjson.JSON;
import com.codeDemo.vo.ParamDto;
import com.codeDemo.vo.Result;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
@Api(tags = "测试API接口")
public class TestController {


    @PostMapping("/test")
    @ApiOperation("测试参数接收类")
    public Result test1(@Validated ParamDto dto) {
        log.info("接收到的参数为：{}", JSON.toJSON(dto));

        return Result.ok();
    }
}
