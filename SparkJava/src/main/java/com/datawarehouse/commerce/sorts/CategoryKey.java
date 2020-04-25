package com.datawarehouse.commerce.sorts;

import scala.math.Ordered;

import java.io.Serializable;

public class CategoryKey implements Ordered<CategoryKey>, Serializable {


    private static final long serialVersionUID = -8219639134524889063L;
    private Long category;
    private Integer click;
    private Integer order;
    private Integer pay;
    public CategoryKey(){}

    public CategoryKey(Long category, Integer click, Integer order, Integer pay) {
        this.category = category;
        this.click = click;
        this.order = order;
        this.pay = pay;
    }



    @Override
    public String toString() {
        return "CategoryKey{" +
                "category=" + category +
                ", click=" + click +
                ", order=" + order +
                ", pay=" + pay +
                '}';
    }

    @Override
    public int compare(CategoryKey that) {
        int click = this.click.compareTo(that.click);
        if(click!=0){
            return click;
        }else {
            int order = this.order.compareTo(that.order);
            if(order!=0){
                return order;
            }else {
                return this.pay.compareTo(that.pay);
            }
        }
    }

    @Override
    public boolean $less(CategoryKey that) {
        int click = this.click.compareTo(that.click);
        int order = this.order.compareTo(that.order);
        int pay = this.pay.compareTo(that.pay);
        if(click<0){
            return true;
        }else if(click==0&&order<0){
            return true;
        }else if(click==0&&order==0&&pay<0){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater(CategoryKey that) {
        int click = this.click.compareTo(that.click);
        int order = this.order.compareTo(that.order);
        int pay = this.pay.compareTo(that.pay);
        if(click>0){
            return true;
        }else if(click==0&&order>0){
            return true;
        }else if(click==0&&order==0&&pay>0){
            return true;
        }
        return false;
    }

    @Override
    public boolean $less$eq(CategoryKey that) {
        int click = this.click.compareTo(that.click);
        int order = this.order.compareTo(that.order);
        int pay = this.pay.compareTo(that.pay);
        if(click<=0){
            return true;
        }else if(click==0&&order<=0){
            return true;
        }else if(click==0&&order==0&&pay<=0){
            return true;
        }
        return false;
    }

    @Override
    public boolean $greater$eq(CategoryKey that) {
        int click = this.click.compareTo(that.click);
        int order = this.order.compareTo(that.order);
        int pay = this.pay.compareTo(that.pay);
        if(click>=0){
            return true;
        }else if(click==0&&order>=0){
            return true;
        }else if(click==0&&order==0&&pay>=0){
            return true;
        }
        return false;
    }

    @Override
    public int compareTo(CategoryKey that) {
        int click = this.click.compareTo(that.click);
        if(click!=0){
            return click;
        }else {
            int order = this.order.compareTo(that.order);
            if(order!=0){
                return order;
            }else {
                return this.pay.compareTo(that.pay);
            }
        }
    }
}
