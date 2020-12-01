package si.dlabs.jearni;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.jitsi.jigasi.JigasiBundleActivator;
import org.jitsi.service.configuration.ConfigurationService;
import org.jitsi.utils.logging.Logger;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
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
                SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
                sslContext.init(null, null, new SecureRandom());
                factory.useSslProtocol(sslContext);
            }
            catch (NoSuchAlgorithmException | KeyManagementException e)
            {
                logger.error("Something went wrong trying to use SSL", e);
            }

            String username = getUsername();
            String password = getPassword();
            String host = getHost();
            int port = getPort();
            String virtualHost = getVirtualhost();

            factory.setUsername(username);
            factory.setPassword(password);
            factory.setVirtualHost(virtualHost);
            factory.setHost(host);
            factory.setPort(port);
            connection = factory.newConnection();
        }

        return connection;
    }

    private static String getHost()
    {
        return getStringProperty(
                "org.jitsi.jigasi.transcription.MQ_HOST",
                System.getenv("MQ_HOST")
        );
    }

    private static int getPort()
    {
        return getIntProperty(
                "org.jitsi.jigasi.transcription.MQ_PORT",
                Integer.parseInt(System.getenv("MQ_PORT"))
        );
    }

    private static String getVirtualhost()
    {
        return getStringProperty(
                "org.jitsi.jigasi.transcription.MQ_VIRTUALHOST",
                System.getenv("MQ_VIRTUALHOST")
        );
    }

    private static String getUsername()
    {
        return getStringProperty(
                "org.jitsi.jigasi.transcription.MQ_USERNAME",
                System.getenv("MQ_USERNAME")
        );
    }

    private static String getPassword()
    {
        return getStringProperty(
                "org.jitsi.jigasi.transcription.MQ_PASSWORD",
                System.getenv("MQ_PASSWORD")
        );
    }

    private static String getStringProperty(String propertyName, String defaultValue)
    {
        ConfigurationService confService = JigasiBundleActivator.getConfigurationService();

        if (confService != null)
        {
            return confService.getString(propertyName, defaultValue);
        }
        else
        {
            logger.error("Configuration service not available");
            return defaultValue;
        }
    }

    private static int getIntProperty(String propertyName, int defaultValue)
    {
        ConfigurationService confService = JigasiBundleActivator.getConfigurationService();

        if (confService != null)
        {
            return confService.getInt(propertyName, defaultValue);
        }
        else
        {
            logger.error("Configuration service not available");
            return defaultValue;
        }
    }
}
