package si.dlabs.jearni;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.transcribestreaming.model.AudioEvent;
import software.amazon.awssdk.services.transcribestreaming.model.AudioStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class AmazonAudioStreamPublisher implements Publisher<AudioStream>
{
    private final InputStream inputStream;

    public AmazonAudioStreamPublisher(InputStream inputStream)
    {
        this.inputStream = inputStream;
    }

    @Override
    public void subscribe(Subscriber<? super AudioStream> subscriber)
    {
        subscriber.onSubscribe(new SubscriptionImpl(subscriber, inputStream));
    }

    private class SubscriptionImpl implements Subscription
    {
        private static final int CHUNK_SIZE_IN_BYTES = 1024;
        private final ExecutorService executor = Executors.newFixedThreadPool(1);
        private final AtomicLong demand = new AtomicLong(0);

        private final Subscriber<? super AudioStream> subscriber;
        private final InputStream inputStream;

        public SubscriptionImpl(Subscriber<? super AudioStream> subscriber, InputStream inputStream)
        {
            this.subscriber = subscriber;
            this.inputStream = inputStream;
        }

        @Override
        public void request(long n)
        {
            if (n <= 0)
            {
                subscriber.onError(new IllegalArgumentException("Demand must be positive."));
            }

            demand.getAndAdd(n);

            executor.submit(() -> {
                try
                {
                    do
                    {
                        ByteBuffer audioBuffer = getNextEvent();
                        if (audioBuffer.remaining() > 0)
                        {
                            AudioEvent audioEvent  = audioEventFromBuffer(audioBuffer);
                            subscriber.onNext(audioEvent);
                        }
                        else
                        {
                            subscriber.onComplete();
                            break;
                        }

                    } while(demand.decrementAndGet() > 0);
                }
                catch (Exception e)
                {
                    subscriber.onError(e);
                }
            });
        }

        @Override
        public void cancel()
        {
            // TODO.
        }

        private ByteBuffer getNextEvent()
        {
            ByteBuffer audioBuffer;
            byte[] audioBytes = new byte[CHUNK_SIZE_IN_BYTES];
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

                return audioBuffer;
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        private AudioEvent audioEventFromBuffer(ByteBuffer bb)
        {
            return AudioEvent
                    .builder()
                    .audioChunk(SdkBytes.fromByteBuffer(bb))
                    .build();
        }
    }
}
