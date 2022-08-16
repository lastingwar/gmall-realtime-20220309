package com.atguigu;

import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;

/**
 * @author yhm
 * @create 2022-08-16 10:11
 */
public class Test02_Method {
    public static void main(String[] args) {
        HashSet<String> strings = new HashSet<>();
        strings.add("hello");
        strings.add("hello1");
        strings.add("hello2");
        String s = StringUtils.join(strings,",");
        System.out.println(s);
    }
}
