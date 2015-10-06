package au.com.redhat.test;

/**
 * Created by admin on 27/08/15.
 */

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.commons.cli.*;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPOutputStream;

import org.apache.qpid.amqp_1_0.jms.impl.*;

public class MessageProducerCompressed {


    public static void main(String[] args) throws Exception {
        Options options = new Options();

        Option payloadFile_option = Option.builder("f")
                .desc("File containing message payload")
                .hasArg(true)
                .argName("file")
                .hasArg()
                .build();

        Option username_option = Option.builder("u")
                .desc("username")
                .hasArg(true)
                .argName("string")
                .build();

        Option password_option = Option.builder("pw")
                .desc("password")
                .hasArg(true)
                .argName("string")
                .build();

        Option messageCount_option = Option.builder("m")
                .hasArg()
                .required(true)
                .argName("count")
                .desc("Number of messages to be sent")
                .build();

        Option threadCount_option = Option.builder("t")
                .hasArg()
                .required(true)
                .argName("Threads")
                .desc("Number of producer threads to be used")
                .build();

        Option brokerurl_option = Option.builder("b")
                .hasArg()
                .argName("BropkerURL")
                .desc("URL to connect to broker")
                .build();

        Option amqp_host_option = Option.builder("h")
                .hasArg()
                .desc("AMQP Broker Host")
                .build();

        Option amqp_port_option = Option.builder("pt")
                .hasArg()
                .desc("AMQP Broker port")
                .build();

        Option inter_msg_delay_option = Option.builder("s")
                .hasArg()
                .desc("delay in msec between messages")
                .build();

        Option amqp_option = new Option("amqp", "Use AMQP protocol");
        Option compression_option = new Option("c", "Compress message");
        Option persistence_option = new Option("p", "Messages are persistent");

        options.addOption(payloadFile_option);
        options.addOption(compression_option);
        options.addOption(persistence_option);
        options.addOption(messageCount_option);
        options.addOption(threadCount_option);
        options.addOption(amqp_host_option);
        options.addOption(amqp_port_option);
        options.addOption(username_option);
        options.addOption(password_option);
        options.addOption(amqp_option);
        options.addOption(brokerurl_option);
        options.addOption(inter_msg_delay_option);


        CommandLineParser parser = new DefaultParser();
        CommandLine line = null;
        try {
            line = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("MessageProducerCompressed", options);
        }


        String LOG_FILE = line.getOptionValue("f");

        Boolean compressed = line.hasOption("c");
        Boolean persistent = line.hasOption("p");
        Boolean useAMQP = line.hasOption("amqp");

        String amqp_brokerHost = line.getOptionValue("h", "localhost");
        String user = line.getOptionValue("u", "admin");
        String password = line.getOptionValue("pw", "admin");
        String brokerURL = line.getOptionValue("b", "tcp://localhost:61616");

        int threadCount = Integer.valueOf(line.getOptionValue("t", "1"));
        int amqp_brokerPort = Integer.valueOf(line.getOptionValue("pt", "61616"));
        int messages = Integer.parseInt(line.getOptionValue("m", "1"));
        long inter_msg_delay = Long.parseLong(line.getOptionValue("s", "0"));
        String body = "";

        ExecutorService execpool = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);


        System.out.println(persistent ? "Using persistent messages" : "Using non persistent message");
        System.out.println(compressed ? "Using compression" : "Not Using compression");

        if ((LOG_FILE!= null) && (!LOG_FILE.isEmpty() )) {
            System.out.println("Using payload file: " + LOG_FILE);
            body = new String(Files.readAllBytes(Paths.get(LOG_FILE)), StandardCharsets.UTF_8);

            if (compressed) {
                byte[] compressedData = doCompression(body);
                System.out.println("Message size = " + body.length() + " Compressed size = " + compressedData.length);
            }
        }else{
            System.out.println("*******No file found*******");
            System.out.println("Auto generating text messages and disabling compression");
            compressed = false;
            body = "";
        }


        Connection connection = null;
        ConnectionFactory cf = null;
        Destination targetDest = null;

        if (useAMQP) {
            System.out.println("Connecting to broker with AMQP " + amqp_brokerHost + " amqp_brokerPort " + " with u/p " + user + " " + password);
            cf = new ConnectionFactoryImpl(amqp_brokerHost, amqp_brokerPort, user, password);
            targetDest = new QueueImpl("LoadTest");
            connection = cf.createConnection(user, password);
        } else {
            System.out.println("Connecting to broker with Openwire " + brokerURL + " with u/p " + user + " " + password);
            cf = new ActiveMQConnectionFactory(brokerURL);
            targetDest = new ActiveMQQueue("LoadTest");
            connection = cf.createConnection(user, password);
        }

        connection.start();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < threadCount; i++) {
            execpool.submit(new MessageProducerRunnable(messages, connection, targetDest, compressed, persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT, inter_msg_delay, "ProducerThread" + i, latch, body));
        }


        try {
            latch.await();
        } catch (InterruptedException E) {
            System.out.println("Exception " + E.getMessage());
        }

        long endTime = System.currentTimeMillis();

        System.out.println("Time to send " + (messages * threadCount) + " message is " + (endTime - startTime) + " msec");//+" TPS "+(1000/((endTime - startTime)/(messages*threadCount))));
        connection.close();

        execpool.shutdownNow();


        System.out.println("Exiting...");
    }

    private static byte[] doCompression(String inMessage) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(inMessage.length());
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(inMessage.getBytes());
        gzip.close();
        return out.toByteArray();
    }

}

