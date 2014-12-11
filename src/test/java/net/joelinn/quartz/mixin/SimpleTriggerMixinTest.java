package net.joelinn.quartz.mixin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.joelinn.quartz.jobstore.mixin.TriggerMixin;
import org.junit.Before;
import org.junit.Test;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.SimpleTrigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.triggers.SimpleTriggerImpl;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.Assert.assertEquals;

/**
 * Joe Linn
 * 7/15/2014
 */
public class SimpleTriggerMixinTest {
    protected ObjectMapper mapper;

    @Before
    public void setUp(){
        mapper = new ObjectMapper();
        mapper.addMixInAnnotations(SimpleTrigger.class, TriggerMixin.class);
    }

    @Test
    public void serialization(){
        SimpleTrigger trigger = TriggerBuilder.newTrigger()
                .forJob("testJob", "testGroup")
                .withIdentity("testTrigger", "testTriggerGroup")
                .usingJobData("timeout", 5)
                .withDescription("A description!")
                .withSchedule(SimpleScheduleBuilder.repeatHourlyForever())
                .build();

        Map<String, String> triggerMap = mapper.convertValue(trigger, new TypeReference<HashMap<String, String>>() {});

        assertThat(triggerMap, hasKey("name"));
        assertEquals("testTrigger", triggerMap.get("name"));
        assertThat(triggerMap, hasKey("group"));
        assertEquals("testTriggerGroup", triggerMap.get("group"));
        assertThat(triggerMap, hasKey("jobName"));
        assertEquals("testJob", triggerMap.get("jobName"));

        SimpleTriggerImpl simpleTrigger = mapper.convertValue(triggerMap, SimpleTriggerImpl.class);

        assertEquals(trigger.getKey().getName(), simpleTrigger.getKey().getName());
        assertEquals(trigger.getKey().getGroup(), simpleTrigger.getKey().getGroup());
        assertEquals(trigger.getStartTime(), simpleTrigger.getStartTime());
        assertEquals(trigger.getRepeatInterval(), simpleTrigger.getRepeatInterval());
    }
}
