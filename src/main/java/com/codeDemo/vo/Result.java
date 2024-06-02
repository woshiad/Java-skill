/**
 * Copyright (c) 2016-2019 人人开源 All rights reserved.
 *
 * https://www.renren.io
 *
 * 版权所有，侵权必究！
 */

package com.codeDemo.vo;


import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.HashMap;

/**
 * 统一返回结果类
 */
@Data
public class Result {
	@ApiModelProperty(value = "是否成功")
	private Boolean success;
	@ApiModelProperty(value = "返回消息")
	private String message;
	@ApiModelProperty(value = "返回数据")
	private Object  data;

	private Result(){};

	public static Result ok() {
		Result result = new Result();
		result.setSuccess(true);
		result.setMessage("请求成功");
		result.setData(new HashMap<>());
		return result;
	}

	public static Result error() {
		Result result = new Result();
		result.setSuccess(false);
		result.setMessage("请求失败");
		result.setData(new HashMap<>());
		return result;
	}


	public Result success(Boolean success) {
		this.setSuccess(success);
		return this;
	}

	public Result message(String message) {
		this.setMessage(message);
		return this;
	}

	public Result data(Object data) {
		this.setData(data);
		return this;
	}
}
