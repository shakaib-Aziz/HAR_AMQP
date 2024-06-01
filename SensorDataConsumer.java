import com.rabbitmq.client.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SensorDataConsumer {

    private static final String QUEUE_NAME = "sensor_data_queue";
    // private static final String VIRTUAL_HOST = "Sensors";

    public static void main(String[] argv) throws Exception {
        // Step 1: Create a ConnectionFactory and configure it
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.28.99");
        factory.setUsername("test");
        factory.setPassword("test");
        // factory.setVirtualHost(VIRTUAL_HOST);

        // Step 2: Establish a Connection
        Connection connection = factory.newConnection();
        // Step 3: Create a Channel
        Channel channel = connection.createChannel();

        // Step 4: Declare the Queue
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        System.out.println("Waiting for messages. To exit press CTRL+C");

        // Step 5: Define the Consumer and handle the message
        /*
         * DeliverCallback deliverCallback = (consumerTag, delivery) -> {
         * String message = new String(delivery.getBody(), "UTF-8");
         * System.out.println("Received message: " + message);
         * MessageUtils.processMessage(message);
         * // Manually acknowledge the message
         * channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
         * };
         */
        // Step 6: Start consuming messages
        channel.basicConsume(QUEUE_NAME, true, (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println("Received message: " + message);
            MessageUtils.processMessage(message);
        }, consumerTag -> {
        });
        synchronized (SensorDataConsumer.class) {
            SensorDataConsumer.class.wait();
        }
    }
}

class MessageUtils {
    private static final String SOURCE_FOLDER_LOCATION = "C:\\HAR_Server_V1\\Instance Data";

    public static void processMessage(String message) {
        try {
            String source = extractSource(message);
            String userId = extractUserId(message);
            String sensorType = extractSensorType(message);
            String timestamp = extractTimestamp(message);
            // String data = extractData(message);
            // System.out.println("DATA: " + data);
            // System.out.println("MSSG: " + message);

            String directoryPath = getFolderPath(source, userId, sensorType, timestamp);

            boolean success = new java.io.File(directoryPath).mkdirs();
            if (success || new java.io.File(directoryPath).exists()) {
                saveDataToFile(directoryPath, userId, sensorType, timestamp, message);
            } else {
                System.out.println("Failed to create directories: " + directoryPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String extractSource(String data) {
        return extractValue(data, "Source: (\\w+)");
    }

    private static String extractUserId(String data) {
        return extractValue(data, "User ID: (\\w+)");
    }

    private static String extractSensorType(String data) {
        // return extractValue(data, "Sensor Type: (\\w+)");
        return extractValue(data, "Sensor Type: ([^:]+)");
    }

    private static String extractTimestamp(String data) {
        return extractValue(data, "Timestamp: (\\d+)");
    }

    private static String extractData(String data) {
        return extractValue(data, "Sensor Data: (.+)");
    }

    private static String extractValue(String data, String regex) {
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(data);
        return matcher.find() ? matcher.group(1) : "Unknown";
    }

    private static String getFolderPath(String source, String userId, String sensorType, String timestamp) {
        return SOURCE_FOLDER_LOCATION + "\\" + source + "\\" + userId + "_" + sensorType;
    }

    private static void saveDataToFile(String directoryPath, String userId, String sensorType, String timestamp,
            String data) {
        try (BufferedWriter writer = new BufferedWriter(
                new FileWriter(directoryPath + "\\" + userId + "_" + sensorType + ".csv", true))) {
            writer.write(data);
            writer.newLine();
            System.out.println("Data saved to file: " + directoryPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
