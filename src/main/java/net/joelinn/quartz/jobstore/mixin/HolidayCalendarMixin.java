package net.joelinn.quartz.jobstore.mixin;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author Joe Linn
 *         10/30/2016
 */
public class HolidayCalendarMixin {
    @JsonProperty
    private TreeSet<Date> dates;


    @JsonIgnore
    public SortedSet<Date> getExcludedDates() {
        return null;
    }
}
