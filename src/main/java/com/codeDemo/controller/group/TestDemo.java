package com.codeDemo.controller.group;

import com.alibaba.fastjson.JSON;
import com.codeDemo.controller.TestClass2;

import java.io.PipedInputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class TestDemo {

    public static void main(String[] args) {


        PipedInputStream pipedInputStream = new PipedInputStream();

    }

    public static String test() {
        String returnValue = "info";
        try {

            int aa=1;
//            int b =aa/0;
            returnValue = "normal";

            if (returnValue.equals("normal") ) {
                return returnValue;
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            returnValue = "finally";
        }

        return returnValue;
    }


    /**
     * 空指针
     * 类型转换异常
     * 数据下标越界
     * es异常
     * fastjson转换对象异常 如：{}转数组 []转对象
     */
}
