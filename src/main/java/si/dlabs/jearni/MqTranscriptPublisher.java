package si.dlabs.jearni;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.jitsi.jigasi.transcription.*;
import org.jitsi.utils.logging.Logger;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class MqTranscriptPublisher
    implements TranscriptionListener
{
    private final static Logger logger
            = Logger.getLogger(MqTranscriptPublisher.class);

    private Connection mqConnection;

    private Channel pcmAudioChannel;

    private String exchangeName = "speech-transcription";

    private String routingKey = "test";

    public MqTranscriptPublisher()
    {
        try
        {
            mqConnection = RabbitMQConnectionFactory.getConnection();
            pcmAudioChannel = mqConnection.createChannel();
            this.configureMq();
        }
        catch (IOException e)
        {
            logger.error("Error establishing connection to message exchange", e);
        }
        catch (TimeoutException e)
        {
            logger.error("Timeout establishing connection to message exchange", e);
        }
    }

    private void configureMq() throws IOException
    {
        String queueName = "speech-transcription-permanent-storage";

        pcmAudioChannel.exchangeDeclare(exchangeName, "fanout", true);
        pcmAudioChannel.queueDeclare(queueName, true, false, false, null);
        pcmAudioChannel.queueBind(queueName, exchangeName, routingKey);
    }

    private void send(String transcript)
    {
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("text/plain")
//                .headers(headers)
                .deliveryMode(2)
                .priority(1)
                .build();

        try
        {
            pcmAudioChannel.basicPublish(
                    exchangeName,
                    routingKey,
                    properties,
                    transcript.getBytes()
            );

            logger.debug("Published a transcript.");
        }
        catch (IOException e)
        {
            logger.error("Exception converting transcript to bytes", e);
        }
    }

    @Override
    public void notify(TranscriptionResult result)
    {
        logger.info("s3 listener notified.");

        if (!result.isInterim())
        {
            StringBuilder txt = new StringBuilder();
            result.getAlternatives().forEach(alt -> {
                txt.append(alt.getTranscription()).append(", ");
            });

            send(txt.toString());
        }
        else
        {
            logger.info("Skipping interim transcription result... TODO: save to something!");
        }
    }

    @Override
    public void completed()
    {
        logger.info("transcription completed");
    }

    @Override
    public void failed(FailureReason reason)
    {
        logger.info("transcription failed");
    }
}
