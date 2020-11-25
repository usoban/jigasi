package si.dlabs.jearni;

import com.rabbitmq.client.*;
import org.jitsi.jigasi.transcription.Participant;
import org.jitsi.jigasi.transcription.Transcriber;
import org.jitsi.utils.logging.Logger;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

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

    public PCMAudioPublisher(Participant participant, int sampleSizeInBits, int sampleRateInHertz)
            throws IOException, TimeoutException
    {
        this(sampleSizeInBits, sampleRateInHertz);
        this.participant = participant;
    }

    public PCMAudioPublisher(int sampleSizeInBits, int sampleRateInHertz)
            throws IOException, TimeoutException
    {
        this.sampleSizeInBits = sampleSizeInBits;
        this.sampleRateInHertz = sampleRateInHertz;
        mqConnection = RabbitMQConnectionFactory.getConnection();
        pcmAudioChannel = mqConnection.createChannel();
        audioBytePipe = new BytePipe(64000);

        configureMq();
        loop();
    }

    private void configureMq() throws IOException
    {
        pcmAudioChannel.exchangeDeclare("amq.direct", "direct", true);
        pcmAudioChannel.queueDeclare("test-audio", true, false, false, null);
        pcmAudioChannel.queueBind("test-audio", "amq.direct", "test-audio");
    }

    public void buffer(byte[] audioBytes) throws IOException
    {
        audioBytePipe.write(audioBytes);
    }

    private void loop()
    {
        int oneSecondBytes = 32000;

        executor.submit(() -> {
            while(true)
            {
                if (audioBytePipe.available() < oneSecondBytes)
                {
                    continue;
                }

                byte[] buff = new byte[oneSecondBytes];
                audioBytePipe.read(buff);

                Map<String, Object> headers = new HashMap<>();
                headers.put("sample_rate", this.sampleRateInHertz);
                headers.put("sample_size_in_bits", this.sampleSizeInBits);
                // TODO: order_index or message plz.
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

                logger.debug("Published a message with 1s of audio.");

                pcmAudioChannel.basicPublish(
                        "amq.direct",
                        "test-audio",
                        properties,
                        buff
                );
            }
        });
    }
}
