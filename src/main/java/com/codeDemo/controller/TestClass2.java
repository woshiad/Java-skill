package com.codeDemo.controller;

import com.alibaba.fastjson.JSON;
import lombok.Data;

@Data
public class TestClass2  extends TestClass{

    private String BB;
    private Integer age;

    @Override
    public TestClass2 tt() {
        TestClass tt = super.tt();
        System.out.println(JSON.toJSON(tt));
        TestClass2 testClass2 = new TestClass2();
        testClass2.setAge(28);
        testClass2.setBB("NameBB");

        return testClass2;
    }
}
