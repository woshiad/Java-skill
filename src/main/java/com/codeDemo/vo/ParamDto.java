package com.codeDemo.vo;


import com.codeDemo.controller.group.InsertGroup;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Date;

/**
 * 参数接收类
 */
@Data
public class ParamDto {

    @NotNull(message = "必填填写姓名", groups = {InsertGroup.class})
    @ApiModelProperty(value = "姓名", example = "张三")
    private String name;

    @ApiModelProperty(value = "年龄", example = "18")
    private Integer age;

    @NotBlank(message = "地址为必填项")
    @ApiModelProperty(value = "地址", example = "四川省成都市")
    private String address;

    @ApiModelProperty(value = "开始时间", example = "格式：YYYY-MM-dd")
    private Date statTime;

    @ApiModelProperty(value = "结束时间", example = "格式：YYYY-MM-d")
    private Date endTime;
}
