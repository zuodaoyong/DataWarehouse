package com.datawarehouse.commerce;

import com.datawarehouse.commerce.bean.ProductInfo;
import com.datawarehouse.commerce.bean.UserInfo;
import com.datawarehouse.commerce.bean.UserVisitAction;
import com.datawarehouse.utils.CommonUtils;
import com.datawarehouse.utils.TimeUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

import java.util.*;

public class MockData {


    public static void main(String[] args) {

        // 创建Spark配置
        SparkConf sparkConf = new SparkConf().setAppName("MockData").setMaster("local[*]");

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();

        List<UserVisitAction> userVisitActions = mockUserVisitActionData();
        List<UserInfo> userInfos = mockUserInfo();
        List<ProductInfo> productInfos = mockProductInfo();


    }

    /**
     * 模拟产品数据表
     *
     * @return
     */
    private static List<ProductInfo> mockProductInfo(){

        List<ProductInfo> rows = new ArrayList<>();
        Random random = new Random();
        List<Integer> productStatus = Arrays.asList(new Integer[]{0, 1});

        // 随机产生100个产品信息
        for (int i =0;i<100;i++) {
            Long productId = Long.valueOf(i);
            String productName = "product" + i;
            String extendInfo = "{\"product_status\": " + productStatus.get(random.nextInt(2)) + "}"
            rows.add(new ProductInfo(productId, productName, extendInfo));
        }
        return rows;
    }

    /**
     * 模拟用户信息表
     *
     * @return
     */
    private static List<UserInfo> mockUserInfo() {

        List<UserInfo> rows = new ArrayList<>();
        List<String> sexes = Arrays.asList(new String[]{"male", "female"});
        Random random = new Random();

        // 随机产生100个用户的个人信息
        for (int i=0;i<100;i++) {
            Long userid = Long.valueOf(i);
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);
            String professional = "professional" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes.get(random.nextInt(2));
            rows.add(new UserInfo(userid, username, name, age,
                    professional, city, sex));
        }
        return rows;
    }

    /**
     * 模拟用户行为信息
     *
     * @return
     */
    private static List<UserVisitAction> mockUserVisitActionData() {

        List<String> searchKeywords = Arrays.asList(new String[]{"华为手机", "联想笔记本", "小龙虾", "卫生纸", "吸尘器", "Lamer", "机器学习", "苹果", "洗面奶", "保温杯"});
        // yyyy-MM-dd

        String date=TimeUtils.parseToFormatTime(new Date(),TimeUtils.DAYSTR);
        // 关注四个行为：搜索、点击、下单、支付
        List<String> actions = Arrays.asList(new String[]{"search", "click", "order", "pay"});
        Random random = new Random();
        List<UserVisitAction> rows = new ArrayList<>();
        // 一共100个用户（有重复）
        for (int i= 0;i< 100;i++) {
            Long userid = Long.valueOf(random.nextInt(100));
            // 每个用户产生10个session
            for (int j=0;j<10;j++) {
                // 不可变的，全局的，独一无二的128bit长度的标识符，用于标识一个session，体现一次会话产生的sessionId是独一无二的
                String sessionid = UUID.randomUUID().toString().replace("-", "");
                // 在yyyy-MM-dd后面添加一个随机的小时时间（0-23）
                String baseActionTime = date + " " + random.nextInt(23);
                // 每个(userid + sessionid)生成0-100条用户访问数据
                for (int k =0;k<random.nextInt(100);k++) {
                    Long pageid = Long.valueOf(random.nextInt(10));
                    // 在yyyy-MM-dd HH后面添加一个随机的分钟时间和秒时间
                    String actionTime = baseActionTime + ":" + CommonUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + CommonUtils.fulfuill(String.valueOf(random.nextInt(59)));
                    String searchKeyword= null;
                    Long clickCategoryId=-1L;
                    Long clickProductId= -1L;
                    String orderCategoryIds= null;
                    String orderProductIds= null;
                    String payCategoryIds= null;
                    String payProductIds= null;
                    Long cityid = Long.valueOf(random.nextInt(10));
                    // 随机确定用户在当前session中的行为
                    String action = actions.get(random.nextInt(4));

                    // 根据随机产生的用户行为action决定对应字段的值
                    switch (action){
                        case "search":
                            searchKeyword=searchKeywords.get(random.nextInt(10));
                            break;
                        case "click":
                            clickCategoryId=Long.valueOf(random.nextInt(100));
                            clickProductId=Long.valueOf(random.nextInt(100));
                            break;
                        case "order":
                            orderCategoryIds=String.valueOf(random.nextInt(100));
                            orderProductIds=String.valueOf(random.nextInt(100));
                            break;
                        case "pay":
                            payCategoryIds=String.valueOf(random.nextInt(100));
                            payProductIds=String.valueOf(random.nextInt(100));
                            break;
                    }
                    rows.add(new UserVisitAction(date, userid, sessionid,
                            pageid, actionTime, searchKeyword,
                            clickCategoryId, clickProductId,
                            orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds, cityid));
                }
            }
        }
        return rows;
    }
}
