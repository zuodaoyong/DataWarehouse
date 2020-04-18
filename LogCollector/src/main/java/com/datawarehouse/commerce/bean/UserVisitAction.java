package com.datawarehouse.commerce.bean;

import java.io.Serializable;

/**
 * 用户访问动作表
 *
 * @param date               用户点击行为的日期
 * @param user_id            用户的ID
 * @param session_id         Session的ID
 * @param page_id            某个页面的ID
 * @param action_time        点击行为的时间点
 * @param search_keyword     用户搜索的关键词
 * @param click_category_id  某一个商品品类的ID
 * @param click_product_id   某一个商品的ID
 * @param order_category_ids 一次订单中所有品类的ID集合
 * @param order_product_ids  一次订单中所有商品的ID集合
 * @param pay_category_ids   一次支付中所有品类的ID集合
 * @param pay_product_ids    一次支付中所有商品的ID集合
 * @param city_id            城市ID
 */
public class UserVisitAction implements Serializable {

    private static final long serialVersionUID = -8534894349701323965L;
    private String date;
    private Long user_id;
    private String session_id;
    private Long page_id;
    private String action_time;
    private String search_keyword;
    private Long click_category_id;
    private Long click_product_id;
    private String order_category_ids;
    private String order_product_ids;
    private String pay_category_ids;
    private String pay_product_ids;
    private Long city_id;

    public UserVisitAction(String date, Long user_id, String session_id, Long page_id, String action_time, String search_keyword, Long click_category_id, Long click_product_id, String order_category_ids, String order_product_ids, String pay_category_ids, String pay_product_ids, Long city_id) {
        this.date = date;
        this.user_id = user_id;
        this.session_id = session_id;
        this.page_id = page_id;
        this.action_time = action_time;
        this.search_keyword = search_keyword;
        this.click_category_id = click_category_id;
        this.click_product_id = click_product_id;
        this.order_category_ids = order_category_ids;
        this.order_product_ids = order_product_ids;
        this.pay_category_ids = pay_category_ids;
        this.pay_product_ids = pay_product_ids;
        this.city_id = city_id;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public String getSession_id() {
        return session_id;
    }

    public void setSession_id(String session_id) {
        this.session_id = session_id;
    }

    public Long getPage_id() {
        return page_id;
    }

    public void setPage_id(Long page_id) {
        this.page_id = page_id;
    }

    public String getAction_time() {
        return action_time;
    }

    public void setAction_time(String action_time) {
        this.action_time = action_time;
    }

    public String getSearch_keyword() {
        return search_keyword;
    }

    public void setSearch_keyword(String search_keyword) {
        this.search_keyword = search_keyword;
    }

    public Long getClick_category_id() {
        return click_category_id;
    }

    public void setClick_category_id(Long click_category_id) {
        this.click_category_id = click_category_id;
    }

    public Long getClick_product_id() {
        return click_product_id;
    }

    public void setClick_product_id(Long click_product_id) {
        this.click_product_id = click_product_id;
    }

    public String getOrder_category_ids() {
        return order_category_ids;
    }

    public void setOrder_category_ids(String order_category_ids) {
        this.order_category_ids = order_category_ids;
    }

    public String getOrder_product_ids() {
        return order_product_ids;
    }

    public void setOrder_product_ids(String order_product_ids) {
        this.order_product_ids = order_product_ids;
    }

    public String getPay_category_ids() {
        return pay_category_ids;
    }

    public void setPay_category_ids(String pay_category_ids) {
        this.pay_category_ids = pay_category_ids;
    }

    public String getPay_product_ids() {
        return pay_product_ids;
    }

    public void setPay_product_ids(String pay_product_ids) {
        this.pay_product_ids = pay_product_ids;
    }

    public Long getCity_id() {
        return city_id;
    }

    public void setCity_id(Long city_id) {
        this.city_id = city_id;
    }
}
