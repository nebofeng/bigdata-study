package com.nebo.kafka_study.an2.model;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

/**
 * Created by John on 2017/4/25.
 */
public class UserBehaviorRequestModel {
    @JsonProperty("begintime")
    private Long beginTime;

    @JsonProperty("endtime")
    private Long endTime;

    @JsonProperty("userId")
    private long userId;

    @JsonProperty("day")
    private String day;

    @JsonProperty("data")
    private List<SingleUserBehaviorRequestModel> dataList;

    public Long getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(Long beginTime) {
        this.beginTime = beginTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public List<SingleUserBehaviorRequestModel> getDataList() {
        return dataList;
    }

    public void setDataList(List<SingleUserBehaviorRequestModel> dataList) {
        this.dataList = dataList;
    }
}
