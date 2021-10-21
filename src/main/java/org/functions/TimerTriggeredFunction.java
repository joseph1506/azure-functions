package org.functions;

import java.sql.*;
import java.time.*;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudAppendBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

/**
 * Azure Functions with Timer trigger.
 */
public class TimerTriggeredFunction {
    /**
     * This function will be invoked periodically according to the specified schedule.
     */
    @FunctionName("TimerTriggeredFunction")
    public void run(
        @TimerTrigger(name = "timerInfo", schedule = "0 */10 * * * *") String timerInfo,
        final ExecutionContext context
    ) {
        String connectionUrl =
                "<<COnfigure>>";

        ResultSet resultSet = null;

        try (Connection connection = DriverManager.getConnection(connectionUrl);
             Statement statement = connection.createStatement();) {

            context.getLogger().info("Start");

            String emailAlertsToSendSql = "select id,mail_body,mail_subject,to_address from email_alerts where email_sent=0";
            resultSet = statement.executeQuery(emailAlertsToSendSql);

            // Print results from select statement
            while (resultSet.next()) {
                String id= resultSet.getString(1);
                String mail_body= resultSet.getString(2);
                String mail_subject= resultSet.getString(3);
                String to_address= resultSet.getString(4);
                //System.out.println(resultSet.getString(2) + " " + resultSet.getString(3));
                writeToFile(id+"-"+mail_body+"-"+mail_subject+"-"+to_address,id);
                updateEmailSentFlag(connection,id);
                context.getLogger().info("Test 2"); // This is never printed.

            }
        }
        // Handle any errors that may have occurred.
        catch (SQLException e) {
            e.printStackTrace();
        }
        context.getLogger().info("Java Timer trigger function executed at: " + LocalDateTime.now());
    }

    private void writeToFile(String content,String id) {
        try {
            String connectionString="<<COnfigure>>";
            // Retrieve storage account from connection-string.
            CloudStorageAccount storageAccount = CloudStorageAccount.parse(connectionString);
            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
            CloudBlobContainer container = blobClient.getContainerReference("emails");
            // Create the container if it does not exist.
            container.createIfNotExists();
            CloudAppendBlob cloudAppendBlob= container.getAppendBlobReference(id);
            cloudAppendBlob.createOrReplace();
            cloudAppendBlob.appendText(content);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void updateEmailSentFlag(Connection connection, String id) {
        try (Statement statement = connection.createStatement();) {
            String emailAlertsToSendSql = "update email_alerts set email_sent=1 where id="+id;
            statement.executeQuery(emailAlertsToSendSql);
        }catch (SQLException e) {
            //e.printStackTrace();
        }
    }
}
