package com;

import com.alibaba.fastjson.JSONObject;
import com.datawarehouse.commerce.bean.UserVisitAction;

public class Test {


    public static void main(String[] args) {
        String str="{\"keyWords\":[\"联想笔记本\"],\"city\":\"city48\",\"click_category\":[-1,34,53,72,90,93,95],\"sex\":\"female\",\"visitLen\":3419,\"startTime\":\"2020-04-18 20:01:43\",\"sessionId\":\"9da87ce93a9b48898c2538826b011d50\",\"endTime\":\"2020-04-18 20:58:42\",\"stepLen\":12,\"userId\":39,\"age\":12,\"professional\":\"professional18\"}";

        UserVisitAction userVisitAction = JSONObject.parseObject(str, UserVisitAction.class);
        System.out.println(userVisitAction);
    }
}
