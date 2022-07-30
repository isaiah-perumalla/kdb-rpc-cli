package system;

import net.kdb4j.KdbConnection;
import net.kdb4j.io.TcpTransportPoller;

import java.io.IOException;

public class SystemTests {


    public static void main(String[] args) throws IOException, InterruptedException {
        TestDataPublisher testDataPublisher = new TestDataPublisher();
        KdbConnection connection = new KdbConnection("isaiahp", testDataPublisher);
        KdbConnection sub = new KdbConnection("isaiahp", new TestDataSubscriber());
        TcpTransportPoller tcpTransportPoller = new TcpTransportPoller();

        tcpTransportPoller.addEndpoint(0, "localhost", 5010, connection);
        tcpTransportPoller.addEndpoint(20,"localhost", 5010, sub);
        while(true) {
            tcpTransportPoller.pollEndpoints();
            Thread.sleep(2000);
            if(testDataPublisher.isClosed()) {
                break;
            }
            testDataPublisher.publish();
        }
    }

}
