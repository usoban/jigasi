package si.dlabs.jearni;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.jitsi.utils.logging.Logger;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class RabbitMQConnectionFactory
{
    private static final ConnectionFactory factory = new ConnectionFactory();
    private static Connection connection;
    private final static Logger logger = Logger.getLogger(RabbitMQConnectionFactory.class);

    public static Connection getConnection()
            throws IOException, TimeoutException
    {
        if (connection == null)
        {
            try
            {
                factory.useSslProtocol();
            }
            catch (NoSuchAlgorithmException| KeyManagementException e)
            {
                logger.error("Something went wrong trying to use SSL", e);
            }

            String host = System.getenv("MQ_HOST");
            int port = Integer.parseInt(System.getenv("MQ_PORT"));
            String virtualHost = System.getenv("MQ_VIRTUALHOST");
            String username = System.getenv("MQ_USERNAME");
            String password = System.getenv("MQ_PASSWORD");

            factory.setUsername(username);
            factory.setPassword(password);
            factory.setVirtualHost(virtualHost);
            factory.setHost(host);
            factory.setPort(port);
            connection = factory.newConnection();
        }

        return connection;
    }
}
