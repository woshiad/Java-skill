package com.codeDemo.vo;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.util.Date;

/**
 * @author LJF
 */
@Data
@TableName("bus_user_salary")
public class Salary {

    /**
     * 部门名
     */
    private String deptName;

    /**
     * 用户名
     */
    private String userName;

    /**
     * 实发
     */
    private String shouldSalary;

    /**
     * 公积金-个人
     */
    private String fundPersonal;

    /**
     * 公积金-公司
     */
    private String fundCompany;

    /**
     * 社保-个人
     */
    private String insurancePersonal;

    /**
     * 社保-公司
     */
    private String insuranceCompany;

    /**
     * 个税
     */
    private String taxRevenue;

    /**
     * 年份
     */
    private String year;

    /**
     * 月份
     */
    private String month;

    /**
     * 创建时间
     */
    private Date createTime;


}
