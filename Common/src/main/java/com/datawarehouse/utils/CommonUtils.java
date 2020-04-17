package com.datawarehouse.utils;

public class CommonUtils {

    public static String fulfuill(String str) {
        if(str.length() == 2) {
           return str;
        } else {
            return "0" + str;
        }
    }
}
