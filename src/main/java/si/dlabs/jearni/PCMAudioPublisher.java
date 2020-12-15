package si.dlabs.jearni;

import com.rabbitmq.client.*;
import org.jitsi.jigasi.transcription.Participant;
import org.jitsi.utils.logging.Logger;
import javax.media.format.AudioFormat;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class PCMAudioPublisher
{
    private static final String EXCHANGE_NAME = "audio";

    private static final Logger logger = Logger.getLogger(PCMAudioPublisher.class);

    private final Participant participant;

    private Channel pcmAudioChannel;

    private BytePipe audioBytePipe;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private int sampleSizeInBits;

    private int sampleRateInHertz;

    private int nBytesForOneSecond;

    private final AtomicBoolean isConfigured = new AtomicBoolean(false);

    private final AtomicInteger messageCounter = new AtomicInteger(0);

    private final AtomicBoolean shutdownLoop = new AtomicBoolean(false);

    public PCMAudioPublisher(Participant participant)
            throws IOException, TimeoutException
    {
        this.participant = participant;

        configureMq();
        loop();
    }

    public void configureAudioFormat(AudioFormat audioFormat)
            throws IOException
    {
        sampleSizeInBits = audioFormat.getSampleSizeInBits();
        sampleRateInHertz = (int) audioFormat.getSampleRate();
        nBytesForOneSecond = sampleRateInHertz * (sampleSizeInBits/8);
        audioBytePipe = new BytePipe(nBytesForOneSecond * 5); // 5s of buffer

        logger.info("Configured PCMAudioPublisher; sample_size = " + sampleSizeInBits + ", sample_rate = " + sampleRateInHertz);

        isConfigured.set(true);
    }

    private void configureMq()
            throws IOException, TimeoutException
    {
        Connection mqConnection = RabbitMQConnectionFactory.getConnection();
        pcmAudioChannel = mqConnection.createChannel();
        pcmAudioChannel.exchangeDeclarePassive(EXCHANGE_NAME);
    }

    public void buffer(byte[] audioBytes)
            throws IOException
    {
        audioBytePipe.write(audioBytes);
    }

    private void loop()
    {
        executor.submit(() -> {
            while(true)
            {
                if (!isConfigured.get())
                {
                    if (shutdownLoop.get())
                    {
                        logger.info("PCMAudioPublisher loop shutting down.");
                        break;
                    }
                    else
                    {
                        continue;
                    }
                }

                try
                {
                    if (audioBytePipe.available() < nBytesForOneSecond)
                    {
                        if (shutdownLoop.get())
                        {
                            logger.info("PCMAudioPublisher loop shutting down.");
                            break;
                        }
                        else
                        {
                            continue;
                        }
                    }

                    byte[] audioData = new byte[nBytesForOneSecond];
                    audioBytePipe.read(audioData);

                    publishAudioMessage(audioData);
                }
                catch (Exception e)
                {
                    logger.error("Error in PCMAudioPublisher", e);
                }
            }
        });
    }

    private void publishAudioMessage(byte[] audioData)
            throws IOException
    {
        String participantId = participant.getId();
        String conferenceId = Utils.getCleanRoomName(participant);
        String routingKey = conferenceId + "." + participantId;

        Map<String, Object> headers = new HashMap<>();
        headers.put("speaker_id", participantId);
        headers.put("speaker_name", this.participant.getName());
        headers.put("conference_id", conferenceId);
        headers.put("sample_rate", this.sampleRateInHertz);
        headers.put("sample_size_in_bits", this.sampleSizeInBits);
        headers.put("order_index", this.messageCounter.getAndIncrement());

        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType("application/octet-stream")
                .headers(headers)
                .deliveryMode(2)
                .priority(1)
                .build();

        pcmAudioChannel.basicPublish(EXCHANGE_NAME, routingKey, properties, audioData);
    }

    public void end()
    {
        logger.info("Shutting down PCMAudioPublisher...");

        if (pcmAudioChannel != null)
        {
            try
            {
                pcmAudioChannel.close();
            }
            catch (IOException e)
            {
                logger.error("Error closing MQ channel", e);
            }
            catch (TimeoutException e)
            {
                logger.error("Timed out while closing MQ channel", e);
            }
        }

        RabbitMQConnectionFactory.releaseConnection();

        executor.shutdown();
        shutdownLoop.set(true);

        logger.info("PCMAudioPublisher set for shutdown.");
    }
}
