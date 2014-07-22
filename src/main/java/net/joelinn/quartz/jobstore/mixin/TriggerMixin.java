package net.joelinn.quartz.jobstore.mixin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.quartz.*;

import java.util.Date;

/**
 * Joe Linn
 * 7/15/2014
 */
public abstract class TriggerMixin {
    @JsonIgnore
    public abstract TriggerBuilder getTriggerBuilder();

    @JsonIgnore
    public abstract JobDataMap getJobDataMap();

    @JsonIgnore
    public abstract JobKey getJobKey();

    @JsonIgnore
    public abstract TriggerKey getKey();

    @JsonIgnore
    public abstract String getFullName();

    @JsonIgnore
    public abstract String getFullJobName();

    @JsonIgnore
    public abstract Date getFinalFireTime();

    @JsonIgnore
    public abstract ScheduleBuilder getScheduleBuilder();
}
