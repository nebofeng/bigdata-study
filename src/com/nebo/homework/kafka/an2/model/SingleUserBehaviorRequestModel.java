package com.nebo.homework.kafka.an2.model;

import org.codehaus.jackson.annotate.JacksonAnnotation;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by John on 2017/4/25.
 */
public class SingleUserBehaviorRequestModel {
    @JsonProperty("package")
    private  String packageName;

    @JsonProperty("activetime")
    private long activeTime;

    public String getPackageName() {
        return packageName;
    }

    public void setPackageName(String packageName) {
        this.packageName = packageName;
    }

    public long getActiveTime() {
        return activeTime;
    }

    public void setActiveTime(long activeTime) {
        this.activeTime = activeTime;
    }
}
