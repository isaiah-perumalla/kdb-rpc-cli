package system;

import net.kdb4j.KdbConnection;
import net.kdb4j.io.TcpTransportPoller;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.SleepingIdleStrategy;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SystemTests {

    private static IdleStrategy idleStrategy = new SleepingIdleStrategy(TimeUnit.SECONDS.toNanos(2));
    public static void main(String[] args) throws IOException, InterruptedException {
        TestDataPublisher testDataPublisher = new TestDataPublisher();
        KdbConnection connection = new KdbConnection("isaiahp", testDataPublisher);
        KdbConnection sub = new KdbConnection("isaiahp", new TestDataSubscriber());
        TcpTransportPoller tcpTransportPoller = new TcpTransportPoller();

        tcpTransportPoller.addEndpoint(0, "localhost", 5010, connection);
        tcpTransportPoller.addEndpoint(20,"localhost", 5010, sub);
        while (true) {
            if (0 == tcpTransportPoller.poll()) {
                idleStrategy.idle();
                testDataPublisher.publish();
            }
            else if(testDataPublisher.isClosed()) {
                break;
            }

        }
    }

}
