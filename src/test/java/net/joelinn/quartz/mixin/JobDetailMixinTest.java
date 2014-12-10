package net.joelinn.quartz.mixin;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.joelinn.quartz.TestJob;
import net.joelinn.quartz.jobstore.mixin.JobDetailMixin;
import org.junit.Before;
import org.junit.Test;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.impl.JobDetailImpl;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.junit.Assert.assertEquals;

/**
 * Joe Linn
 * 7/15/2014
 */
public class JobDetailMixinTest {
    protected ObjectMapper mapper;

    @Before
    public void setUp(){
        mapper = new ObjectMapper();
        mapper.addMixInAnnotations(JobDetail.class, JobDetailMixin.class);
    }

    @Test
    public void testSerializeJobDetail() throws Exception {
        JobDetail testJob = JobBuilder.newJob(TestJob.class)
                .withIdentity("testJob", "testGroup")
                .usingJobData("timeout", 42)
                .withDescription("I am describing a job!")
                .build();

        String json = mapper.writeValueAsString(testJob);
        Map<String, Object> jsonMap = mapper.readValue(json, new TypeReference<HashMap<String, String>>() {
        });

        assertThat(jsonMap, hasKey("name"));
        assertEquals(testJob.getKey().getName(), jsonMap.get("name"));
        assertThat(jsonMap, hasKey("group"));
        assertEquals(testJob.getKey().getGroup(), jsonMap.get("group"));
        assertThat(jsonMap, hasKey("jobClass"));
        assertEquals(testJob.getJobClass().getName(), jsonMap.get("jobClass"));

        JobDetailImpl jobDetail = mapper.readValue(json, JobDetailImpl.class);

        assertEquals(testJob.getKey().getName(), jobDetail.getKey().getName());
        assertEquals(testJob.getKey().getGroup(), jobDetail.getKey().getGroup());
        assertEquals(testJob.getJobClass(), jobDetail.getJobClass());
    }
}
