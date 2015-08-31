package au.com.redhat.test;

/**
 * Created by admin on 27/08/15.
 */

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

public class MessageProducerCompressed {


    public static void main(String[] args) throws Exception {

        String LOG_FILE = args[0];
        String messageCount = arg(args, 1, "1");
        Boolean compressed = Boolean.valueOf(arg(args, 2, "false"));
        Boolean persistent = Boolean.valueOf(arg(args, 3, "true"));
        int threadCount = Integer.valueOf(arg(args, 4, "3"));
        String user = env("ACTIVEMQ_USER", "admin");
        String password = env("ACTIVEMQ_PASSWORD", "admin");
        String host = env("ACTIVEMQ_HOST", "192.168.1.240");
        int port = Integer.parseInt(env("ACTIVEMQ_PORT", "61616"));
//        int port = Integer.parseInt(env("ACTIVEMQ_PORT", "5672"));
        String destination = "SampleTest";

        ExecutorService execpool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        if (LOG_FILE.isEmpty() || LOG_FILE == null) {
            System.out.println("No payload file passed " + LOG_FILE);
            System.exit(0);
        }

        System.out.println(persistent ? "Using persistent messages":"Using non persistent message");

        System.out.println("Using log file: " + LOG_FILE);

        String body = new String(Files.readAllBytes(Paths.get(LOG_FILE)), StandardCharsets.UTF_8);

        int messages = Integer.parseInt(messageCount);

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://" + host + ":" + port);

        Connection connection = factory.createConnection(user, password);
        connection.start();


        System.out.println(compressed ? "Using compression" : "Not Using compression");

        if (compressed) {
            byte[] compressedData = doCompression(body);
            System.out.println("Message size = " + body.length() + " Compressed size = " + compressedData.length);
        }


        long startTime = System.currentTimeMillis();

        for (int i =0;i<threadCount;i++){
            execpool.submit(new MessageProducerRunnable(messages, connection, destination, compressed, persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT, "mythread" + i, latch, body));
        }


        try {
            latch.await();
        } catch (InterruptedException E) {
            System.out.println("Exception "+E.getMessage());
        }

        long endTime = System.currentTimeMillis();

        System.out.println("Time to send " + (messages * threadCount) + " message is " + (endTime - startTime) + " msec");//+" TPS "+(1000/((endTime - startTime)/(messages*threadCount))));
        connection.close();

        execpool.shutdownNow();


        System.out.println("Exiting...");

    }

    private static String env(String key, String defaultValue) {
        String rc = System.getenv(key);
        if (rc == null)
            return defaultValue;
        return rc;
    }

    private static String arg(String[] args, int index, String defaultValue) {
        if (index < args.length)
            return args[index];
        else
            return defaultValue;
    }

    private static byte[] doCompression(String inMessage) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(inMessage.length());
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(inMessage.getBytes());
        gzip.close();
        return out.toByteArray();
    }

}

