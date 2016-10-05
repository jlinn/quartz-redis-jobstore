package net.joelinn.quartz;

import org.quartz.*;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * @author Joe Linn
 *         10/4/2016
 */
public class TestUtils {
    private TestUtils() {}

    public static int getPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            socket.setReuseAddress(true);
            return socket.getLocalPort();
        }
    }


    public static JobDetail createJob(Class<? extends Job> jobClass, String name, String group) {
        return JobBuilder.newJob(jobClass)
                .withIdentity(name, group)
                .build();
    }


    public static CronTrigger createCronTrigger(String name, String group, String cron) {
        return TriggerBuilder.newTrigger()
                .withIdentity(name, group)
                .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                .build();
    }
}
