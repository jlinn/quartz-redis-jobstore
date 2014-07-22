package net.joelinn.quartz.mixin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.joelinn.quartz.jobstore.mixin.CronTriggerMixin;
import org.junit.Before;
import org.junit.Test;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.triggers.CronTriggerImpl;

import java.util.HashMap;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasKey;

/**
 * Joe Linn
 * 7/15/2014
 */
public class CronTriggerMixinTest {
    protected ObjectMapper mapper;

    @Before
    public void setUp(){
        mapper = new ObjectMapper();
        mapper.addMixInAnnotations(CronTrigger.class, CronTriggerMixin.class);
    }

    @Test
    public void testSerialization(){
        String cron = "0/5 * * * * ?";
        CronTrigger trigger = TriggerBuilder.newTrigger()
                .forJob("testJob", "testGroup")
                .withIdentity("testTrigger", "testTriggerGroup")
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .usingJobData("timeout", 5)
                .withDescription("A description!")
                .build();

        Map<String, String> triggerMap = mapper.convertValue(trigger, new TypeReference<HashMap<String, String>>() {});

        assertThat(triggerMap, hasKey("name"));
        assertEquals("testTrigger", triggerMap.get("name"));
        assertThat(triggerMap, hasKey("group"));
        assertEquals("testTriggerGroup", triggerMap.get("group"));
        assertThat(triggerMap, hasKey("jobName"));
        assertEquals("testJob", triggerMap.get("jobName"));

        CronTriggerImpl cronTrigger = mapper.convertValue(triggerMap, CronTriggerImpl.class);

        assertEquals(trigger.getKey().getName(), cronTrigger.getKey().getName());
        assertEquals(trigger.getKey().getGroup(), cronTrigger.getKey().getGroup());
        assertEquals(trigger.getStartTime(), cronTrigger.getStartTime());
        assertEquals(trigger.getCronExpression(), cronTrigger.getCronExpression());
        assertEquals(trigger.getTimeZone(), cronTrigger.getTimeZone());
    }
}
