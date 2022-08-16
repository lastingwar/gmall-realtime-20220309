package com.atguigu;

import com.atguigu.gmall.util.DateFormatUtil;
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

        long l = 1645427097000L + 1000L * 60 * 60 * 24;
        System.out.println(l);

        System.out.println(DateFormatUtil.toDate(1645513497000L));
    }
}
