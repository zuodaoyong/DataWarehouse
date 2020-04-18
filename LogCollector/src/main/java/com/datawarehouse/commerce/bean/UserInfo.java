package com.datawarehouse.commerce.bean;

import java.io.Serializable;

/**
 * 用户信息表
 *
 * @param user_id      用户的ID
 * @param username     用户的名称
 * @param name         用户的名字
 * @param age          用户的年龄
 * @param professional 用户的职业
 * @param city         用户所在的城市
 * @param sex          用户的性别
 */
public class UserInfo implements Serializable {

    private static final long serialVersionUID = -5104820806035898681L;
    private Long user_id;
    private String username;
    private String name;
    private Integer age;
    private String professional;
    private String city;
    private String sex;

    public UserInfo(Long user_id, String username, String name, Integer age, String professional, String city, String sex) {
        this.user_id = user_id;
        this.username = username;
        this.name = name;
        this.age = age;
        this.professional = professional;
        this.city = city;
        this.sex = sex;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getProfessional() {
        return professional;
    }

    public void setProfessional(String professional) {
        this.professional = professional;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getSex() {
        return sex;
    }

    public void setSex(String sex) {
        this.sex = sex;
    }
}
