package net.joelinn.quartz.jobstore.mixin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobKey;

/**
 * Joe Linn
 * 7/15/2014
 */
public abstract class JobDetailMixin {
     @JsonIgnore
    public abstract JobKey getKey();

    @JsonIgnore
    public abstract JobDataMap getJobDataMap();

    @JsonIgnore
    public abstract JobBuilder getJobBuilder();

    @JsonIgnore
    public abstract String getFullName();

    @JsonIgnore
    public abstract boolean isPersistJobDataAfterExecution();

    @JsonIgnore
    public abstract boolean isConcurrentExectionDisallowed();

    @JsonProperty("durable")
    public abstract void setDurability(boolean d);
}
