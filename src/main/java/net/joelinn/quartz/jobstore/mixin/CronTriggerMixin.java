package net.joelinn.quartz.jobstore.mixin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.quartz.CronExpression;

/**
 * Joe Linn
 * 7/15/2014
 */
public abstract class CronTriggerMixin extends TriggerMixin{
    @JsonIgnore
    public abstract String getExpressionSummary();

    @JsonIgnore
    public abstract void setCronExpression(CronExpression cron);

    @JsonProperty("cronExpression")
    public abstract void setCronExpression(String cronExpression);

    @JsonProperty("cronExpression")
    public abstract String getCronExpression();

}
