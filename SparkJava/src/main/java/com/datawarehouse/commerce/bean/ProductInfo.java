package com.datawarehouse.commerce.bean;

import java.io.Serializable;

/**
 * 产品表
 *
 * @param product_id   商品的ID
 * @param product_name 商品的名称
 * @param extend_info  商品额外的信息
 */

public class ProductInfo implements Serializable {

    private static final long serialVersionUID = -1928280326684738671L;
    private Long product_id;
    private String product_name;
    private String extend_info;

    public ProductInfo(Long product_id, String product_name, String extend_info) {
        this.product_id = product_id;
        this.product_name = product_name;
        this.extend_info = extend_info;
    }

    public Long getProduct_id() {
        return product_id;
    }

    public void setProduct_id(Long product_id) {
        this.product_id = product_id;
    }

    public String getProduct_name() {
        return product_name;
    }

    public void setProduct_name(String product_name) {
        this.product_name = product_name;
    }

    public String getExtend_info() {
        return extend_info;
    }

    public void setExtend_info(String extend_info) {
        this.extend_info = extend_info;
    }
}
