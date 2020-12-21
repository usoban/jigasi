package si.dlabs.jearni;

import org.jitsi.jigasi.transcription.Participant;
import javax.media.format.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.TargetDataLine;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PcmAudioPublisherTest
{
    public static void main(String[] args) throws Exception
    {
        TargetDataLine mic = Microphone.get();
        mic.start();

        AudioInputStream inputStream = new AudioInputStream(mic);
        AudioFormat audioFormat = new AudioFormat(
                mic.getFormat().getEncoding().toString(),
                mic.getFormat().getSampleRate(),
                mic.getFormat().getSampleSizeInBits(),
                mic.getFormat().getChannels()
        );

        System.out.println("Encoding: " + audioFormat.getEncoding());
        System.out.println("Sample rate: " + audioFormat.getSampleRate());
        System.out.println("Sample size in bits: " + audioFormat.getSampleSizeInBits());
        System.out.println("Channels: " + audioFormat.getChannels());

        Participant participant = new Participant(null, "Urban");

//        PCMAudioPublisher pcmAudioPublisher = new PCMAudioPublisher(
//                participant,
//                audioFormat.getSampleSizeInBits(),
//                (int)audioFormat.getSampleRate()
//        );

        ExecutorService executor = Executors.newFixedThreadPool(1);

        executor.submit(() -> {
            byte[] buffer;

            do
            {
                buffer = getNextEvent(inputStream);
//                pcmAudioPublisher.buffer(buffer);
            } while(true);
        });
    }

    private static byte[] getNextEvent(InputStream inputStream)
    {
        ByteBuffer audioBuffer;
        byte[] audioBytes = new byte[1096];
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
