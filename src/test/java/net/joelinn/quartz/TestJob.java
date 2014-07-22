package net.joelinn.quartz;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Joe Linn
 * 7/15/2014
 */
public class TestJob implements Job{
    private static Logger logger = LoggerFactory.getLogger(TestJob.class);

    protected int timeout;

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        logger.info(String.format("Test job running with timeout %s", timeout));
    }
}
