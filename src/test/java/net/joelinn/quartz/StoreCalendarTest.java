package net.joelinn.quartz;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.quartz.Calendar;
import org.quartz.JobDetail;
import org.quartz.JobPersistenceException;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.Assert.*;

/**
 * Joe Linn
 * 7/17/2014
 */
public class StoreCalendarTest extends BaseTest{
    @Test
    public void testStoreCalendar(){
        final String calendarName = "weekdayCalendar";
        Calendar calendar = getCalendar();

        try {
            jobStore.storeCalendar(calendarName, calendar, false, false);
        } catch (JobPersistenceException e) {
            e.printStackTrace();
            fail();
        }

        final String calendarHashKey = schema.calendarHashKey(calendarName);
        Map<String, String> calendarMap = jedis.hgetAll(calendarHashKey);

        assertThat(calendarMap, hasKey("calendar_class"));
        assertEquals(calendar.getClass().getName(), calendarMap.get("calendar_class"));
        assertThat(calendarMap, hasKey("calendar_json"));

        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> calendarJson = mapper.readValue(calendarMap.get("calendar_json"), new TypeReference<HashMap<String, Object>>() {});
            assertThat(calendarJson, hasKey("description"));
            assertEquals("Only run on weekdays.", calendarJson.get("description"));
        } catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    @Test
    public void testStoreCalendarWithReplace() throws JobPersistenceException {
        final String calendarName = "weekdayCalendar";
        Calendar calendar = getCalendar();
        jobStore.storeCalendar(calendarName, calendar, true, false);
        jobStore.storeCalendar(calendarName, calendar, true, false);
    }

    @Test(expected = JobPersistenceException.class)
    public void testStoreCalendarNoReplace() throws JobPersistenceException {
        final String calendarName = "weekdayCalendar";
        Calendar calendar = getCalendar();
        jobStore.storeCalendar(calendarName, calendar, false, false);
        jobStore.storeCalendar(calendarName, calendar, false, false);
    }

    @Test
    public void testRetrieveCalendar() throws JobPersistenceException {
        final String calendarName = "weekdayCalendar";
        Calendar calendar = getCalendar();
        jobStore.storeCalendar(calendarName, calendar, false, false);

        Calendar retrievedCalendar = jobStore.retrieveCalendar(calendarName);

        assertEquals(calendar.getClass(), retrievedCalendar.getClass());
        assertEquals(calendar.getDescription(), retrievedCalendar.getDescription());
        long currentTime = System.currentTimeMillis();
        assertEquals(calendar.getNextIncludedTime(currentTime), retrievedCalendar.getNextIncludedTime(currentTime));
    }

    @Test
    public void testGetNumberOfCalendars() throws JobPersistenceException {
        jobStore.storeCalendar("calendar1", getCalendar(), false, false);
        jobStore.storeCalendar("calendar1", getCalendar(), true, false);
        jobStore.storeCalendar("calendar2", getCalendar(), false, false);

        int numberOfCalendars = jobStore.getNumberOfCalendars();

        assertEquals(2, numberOfCalendars);
    }

    @Test
    public void testGetCalendarNames() throws JobPersistenceException {
        List<String> calendarNames = jobStore.getCalendarNames();

        assertThat(calendarNames, not(nullValue()));
        assertThat(calendarNames, hasSize(0));

        jobStore.storeCalendar("calendar1", getCalendar(), false, false);
        jobStore.storeCalendar("calendar2", getCalendar(), false, false);

        calendarNames = jobStore.getCalendarNames();

        assertThat(calendarNames, hasSize(2));
        assertThat(calendarNames, containsInAnyOrder("calendar2", "calendar1"));
    }

    @Test
    public void testRemoveCalendar() throws JobPersistenceException {
        assertFalse(jobStore.removeCalendar("foo"));

        jobStore.storeCalendar("calendar1", getCalendar(), false, false);

        assertTrue(jobStore.removeCalendar("calendar1"));

        assertThat(jobStore.retrieveCalendar("calendar1"), nullValue());
    }

    @Test(expected = JobPersistenceException.class)
    public void testRemoveCalendarWithTrigger() throws JobPersistenceException {
        // store trigger and job
        JobDetail job = getJobDetail();
        jobStore.storeJob(job, false);
        CronTriggerImpl trigger1 = getCronTrigger("trigger1", "group1", job.getKey());
        jobStore.storeTrigger(trigger1, false);

        jobStore.removeCalendar(trigger1.getCalendarName());
    }
}
