package si.dlabs.jearni;

import com.rabbitmq.client.*;
import org.jitsi.jigasi.transcription.Participant;
import org.jitsi.jigasi.transcription.Transcriber;
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
    private Participant participant;

    private Connection mqConnection;

    private Channel pcmAudioChannel;

    private BytePipe audioBytePipe;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private Logger logger = Logger.getLogger(PCMAudioPublisher.class);

    private int sampleSizeInBits;

    private int sampleRateInHertz;

    private int nBytesForOneSecond;

    private AtomicBoolean isConfigured = new AtomicBoolean(false);

    private AtomicInteger messageCounter = new AtomicInteger(0);

    public PCMAudioPublisher(Participant participant) throws IOException, TimeoutException
    {
        this.participant = participant;
        mqConnection = RabbitMQConnectionFactory.getConnection();
        pcmAudioChannel = mqConnection.createChannel();

        configureMq();
        loop();
    }

    public void configureAudioFormat(AudioFormat audioFormat) throws IOException
    {
        sampleSizeInBits = audioFormat.getSampleSizeInBits();
        sampleRateInHertz = (int) audioFormat.getSampleRate();
        nBytesForOneSecond = sampleRateInHertz * (sampleSizeInBits/8);
        audioBytePipe = new BytePipe(nBytesForOneSecond * 5); // 5s of buffer

        logger.info("Configured PCMAudioPublisher; sample_size = " + sampleSizeInBits + ", sample_rate = " + sampleRateInHertz);

        isConfigured.set(true);
    }

    private void configureMq() throws IOException
    {
        pcmAudioChannel.exchangeDeclare("amq.direct", "direct", true);
        pcmAudioChannel.queueDeclare("test-audio", true, false, false, null);
        pcmAudioChannel.queueBind("test-audio", "amq.direct", "test-audio");
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
                    continue;
                }

                if (audioBytePipe.available() < nBytesForOneSecond)
                {
                    continue;
                }

                byte[] audioData = new byte[nBytesForOneSecond];
                audioBytePipe.read(audioData);

                Map<String, Object> headers = new HashMap<>();
                headers.put("sample_rate", this.sampleRateInHertz);
                headers.put("sample_size_in_bits", this.sampleSizeInBits);
                headers.put("order_index", this.messageCounter.getAndIncrement());

                if (participant != null)
                {
                    headers.put("participant_id", this.participant.getId());
                    headers.put("participant_name", this.participant.getName());

                    Transcriber transcriber = participant.getTranscriber();
                    if (transcriber != null)
                    {
                        headers.put("meeting_room_name", transcriber.getRoomName());
                    }
                }

                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .contentType("application/octet-stream")
                        .headers(headers)
                        .deliveryMode(2)
                        .priority(1)
                        .build();

                pcmAudioChannel.basicPublish(
                        "amq.direct",
                        "test-audio",
                        properties,
                        audioData
                );
            }
        });
    }
}
