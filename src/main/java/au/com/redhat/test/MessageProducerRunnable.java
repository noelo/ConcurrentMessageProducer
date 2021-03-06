package au.com.redhat.test;

import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.*;
import java.io.ByteArrayOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPOutputStream;

/**
 * Created by admin on 28/08/15.
 */
public class MessageProducerRunnable implements Runnable {
    private Connection connection;
    private Session session;
    private int msgCount;
    private MessageProducer producer;
    private Destination dest;
    private boolean compression;
    private String stringPayload;
    private String threadID;
    private CountDownLatch latch;
    private int deliveryMode;
    private long inter_msg_delay;

    public MessageProducerRunnable(int inMsgCount, Connection inConn, Destination inDest, boolean inCompression, int inDeliveryMode, long inter_msg_delay, String inThreadID, CountDownLatch inLatch, String inBody) {
        this.connection = inConn;
        this.msgCount = inMsgCount;
        this.dest = inDest;
        this.compression = inCompression;
        this.deliveryMode = inDeliveryMode;
        this.threadID = inThreadID;
        this.latch = inLatch;
        this.stringPayload = inBody;
        this.inter_msg_delay = inter_msg_delay;
    }


    private void preRun() throws Exception {
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(dest);
        producer.setDeliveryMode(deliveryMode);
    }


    @Override
    public void run() {
        String threadName = Thread.currentThread().getName();

        try {
            preRun();

            for (int i = 1; i <= msgCount; i++) {
                try {
                    if (inter_msg_delay > 0) {
                        Thread.sleep(inter_msg_delay);
                    }
                    if (compression) {
                        byte[] compressedData = doCompression(stringPayload);
                        BytesMessage msg = session.createBytesMessage();
                        msg.writeBytes(compressedData);
                        producer.send(msg);
                    } else {
                        TextMessage msg;
                        if (stringPayload.isEmpty()) {
                            msg = session.createTextMessage("Autogenerate message " + i + " " + System.currentTimeMillis());
                        } else {
                            msg = session.createTextMessage(stringPayload);
                        }
                        producer.send(msg);
                    }

                    if ((i % 1000) == 0) {
                        System.out.println(String.format(threadName + " Sent %d messages", i));
                    }
                } catch (Exception ex) {

                    System.out.println(threadName + " Exception sending message " + ex);
                }
            }

            System.out.println(threadName + " Finished sending " + msgCount + " messages");

            latch.countDown();
            producer.close();
            session.close();


        } catch (Exception ex) {
            System.out.println(threadID + " Exception thrown " + ex);
            return;
        }

    }

    private static byte[] doCompression(String inMessage) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream(inMessage.length());
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(inMessage.getBytes());
        gzip.close();
        return out.toByteArray();
    }
}
