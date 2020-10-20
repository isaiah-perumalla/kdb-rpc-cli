package net.kdb4j;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.util.concurrent.atomic.AtomicBoolean;

public class AgentUtil {

    public static void runAgentUntilSignal(
            final Agent agent) throws InterruptedException
    {

        final AgentRunner runner = new AgentRunner(
                new SleepingIdleStrategy(),
                Throwable::printStackTrace,
                null,
                agent);

        final Thread thread = AgentRunner.startOnThread(runner);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        while (running.get())
        {
            Thread.sleep(100);
        }

        thread.join();
    }
}
