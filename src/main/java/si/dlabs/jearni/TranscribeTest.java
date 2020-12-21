package si.dlabs.jearni;

import javax.media.format.AudioFormat;
import javax.sound.sampled.TargetDataLine;
import javax.sound.sampled.AudioInputStream;
import org.jitsi.jigasi.transcription.TranscriptionRequest;
import org.jitsi.jigasi.transcription.TranscriptionService;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TranscribeTest {
    public static void main(String[] args) throws Exception
    {
        AmazonTranscriptionService transcriptionService = new AmazonTranscriptionService();
        AmazonTranscriptResultPublisher s3TranscriptPublisher = new AmazonTranscriptResultPublisher(null);
        TranscriptionService.StreamingRecognitionSession session = transcriptionService.initStreamingSession(null);

        TargetDataLine mic = Microphone.get();
        mic.start();

        AudioInputStream inputStream = new AudioInputStream(mic);
        AudioFormat audioFormat = new AudioFormat(
                mic.getFormat().getEncoding().toString(),
                mic.getFormat().getSampleRate(),
                mic.getFormat().getSampleSizeInBits(),
                mic.getFormat().getChannels()
        );

        ExecutorService executor = Executors.newFixedThreadPool(1);

        executor.submit(() -> {
            byte[] buffer;
            do {
                buffer = getNextEvent(inputStream);
                TranscriptionRequest request = new TranscriptionRequest(buffer, audioFormat, new Locale("en_US"));
                session.sendRequest(request);
            } while(true);

        });
    }

    private static byte[] getNextEvent(InputStream inputStream)
    {
        ByteBuffer audioBuffer;
        byte[] audioBytes = new byte[1920];
        int len = 0;

        try
        {
            len = inputStream.read(audioBytes);

            if (len <= 0)
            {
                audioBuffer = ByteBuffer.allocate(0);
            }
            else
            {
                audioBuffer = ByteBuffer.wrap(audioBytes, 0, len);
            }

            return audioBuffer.array();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }
}
