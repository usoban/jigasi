package org.jitsi.jigasi.transcription;

import org.jitsi.utils.logging.Logger;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import si.dlabs.jearni.BytePipe;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.transcribestreaming.TranscribeStreamingAsyncClient;
import software.amazon.awssdk.services.transcribestreaming.model.*;
import software.amazon.awssdk.services.transcribestreaming.model.TranscriptEvent;
import javax.media.format.AudioFormat;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class AmazonTranscriptionService
    implements TranscriptionService
{
    public final static String[] SUPPORTED_LANGUAGE_TAGS = new String[] {
            LanguageCode.EN_US.toString(),
            LanguageCode.EN_GB.toString()
    };

    private final static Logger logger
            = Logger.getLogger(AmazonTranscriptionService.class);

    private TranscribeStreamingAsyncClient client;

    public AmazonTranscriptionService()
    {
        client = TranscribeStreamingAsyncClient
                .builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();
    }

    @Override
    public boolean supportsFragmentTranscription() {
        return true;
    }

    @Override
    public void sendSingleRequest(TranscriptionRequest request, Consumer<TranscriptionResult> resultConsumer)
            throws UnsupportedOperationException
    {
        // TODO
        logger.error("sendSingleRequest is not yet implemented.");
    }

    @Override
    public boolean supportsStreamRecognition() {
        return true;
    }

    @Override
    public StreamingRecognitionSession initStreamingSession(Participant participant)
            throws UnsupportedOperationException
    {
        return new AmazonStreamingRecognitionSession(client);
    }

    @Override
    public boolean isConfiguredProperly()
    {
        return false;
    }

    public class AmazonStreamingRecognitionSession implements StreamingRecognitionSession
    {
        private final TranscribeStreamingAsyncClient client;

        private AmazonAudioStreamPublisher audioPublisher;
        private CompletableFuture<Void> streamTranscriptionFuture;
        private List<TranscriptionListener> transcriptionListeners = new LinkedList<TranscriptionListener>();
        private UUID messageId;

        public AmazonStreamingRecognitionSession(TranscribeStreamingAsyncClient client)
        {
            this.client = client;
            messageId = UUID.randomUUID();
        }

        protected StartStreamTranscriptionRequest buildStreamingRequest(TranscriptionRequest request)
        {
            AudioFormat audioFormat = request.getFormat();

            logger.info("Encoding is " + audioFormat.getEncoding());

            if (
                    !audioFormat.getEncoding().equals(AudioFormat.LINEAR) &&
                    !audioFormat.getEncoding().equals("PCM_SIGNED")
            )
            {
                throw new IllegalArgumentException("Audio format encoding not supported: ");
            }

            int sampleRateInHertz = Double.valueOf(audioFormat.getSampleRate()).intValue();

            return StartStreamTranscriptionRequest
                    .builder()
                    .mediaEncoding(MediaEncoding.PCM)
                    .languageCode(LanguageCode.EN_US)
                    .mediaSampleRateHertz(sampleRateInHertz)
                    .build();
        }

        protected StartStreamTranscriptionResponseHandler buildStreamingResponseHandler()
        {
            return StartStreamTranscriptionResponseHandler
                    .builder()
                    .subscriber(buildResponseHandler())
                    .build();
        }

        protected Consumer<TranscriptResultStream> buildResponseHandler()
        {
            return (TranscriptResultStream e) -> {
                TranscriptEvent event = (TranscriptEvent) e;

                logger.info(e.toString());

                if (event.transcript().results().size() < 1)
                {
                    logger.info("Transcription returned no results.");
                    return;
                }

                Result firstResult = event.transcript().results().get(0);

                if (firstResult.alternatives().size() < 1)
                {
                    logger.warn("Transcription result has no alternatives?");
                }

                Alternative firstAlternative = firstResult.alternatives().get(0);
                TranscriptionAlternative transcriptionAlternative = new TranscriptionAlternative(
                        firstAlternative.transcript()
                );
                TranscriptionResult transcriptionResult = new TranscriptionResult(
                        null,
                        messageId,
                        firstResult.isPartial(),
                        "en_US",
                        1.0,
                        transcriptionAlternative
                );

                for (TranscriptionListener listener : transcriptionListeners)
                {
                    listener.notify(transcriptionResult);
                }
            };
        }

        protected void startStreamingRequest(TranscriptionRequest transcriptionRequest) throws IOException
        {
            StartStreamTranscriptionRequest request = buildStreamingRequest(transcriptionRequest);
            StartStreamTranscriptionResponseHandler responseHandler = buildStreamingResponseHandler();
            audioPublisher = new AmazonAudioStreamPublisher();

            logger.info("Starting stream transcription.....");
            streamTranscriptionFuture = client.startStreamTranscription(request, audioPublisher, responseHandler);
            logger.info("Stream transcription started.");
        }

        @Override
        public void sendRequest(TranscriptionRequest request)
        {
            if (ended())
            {
                try
                {
                    startStreamingRequest(request);
                }
                catch (IOException e)
                {
                    logger.error("Error starting streaming request", e);
                }
            }

            audioPublisher.pushAudioBytes(request.getAudio());
        }

        @Override
        public void end()
        {
            // TODO: this is probably wrong? Should we cancel streamTranscriptionFuture?
            this.client.close();
        }

        @Override
        public boolean ended()
        {
            return streamTranscriptionFuture == null || streamTranscriptionFuture.isDone();
        }

        @Override
        public void addTranscriptionListener(TranscriptionListener listener)
        {
            transcriptionListeners.add(listener);
        }
    }

    /**
     * Publishes chunks of audio to the stream.
     */
    public class AmazonAudioStreamPublisher implements Publisher<AudioStream>
    {
        private final BytePipe bytePipe;

        public AmazonAudioStreamPublisher() throws IOException
        {
            bytePipe = new BytePipe();
        }

        @Override
        public void subscribe(Subscriber<? super AudioStream> subscriber)
        {
            subscriber.onSubscribe(new SubscriptionImpl(subscriber, bytePipe));
        }

        public void pushAudioBytes(byte[] bytes)
        {
            try
            {
                bytePipe.write(bytes);
            }
            catch (IOException e)
            {
                logger.error("Error pushing audio bytes to BytePipe", e);
            }
        }
    }

    /**
     * Subscription implementation.
     */
    private class SubscriptionImpl implements Subscription
    {
        private final Subscriber<? super AudioStream> subscriber;
        private final ExecutorService executor = Executors.newSingleThreadExecutor();
        private final AtomicLong demand = new AtomicLong(0);
        private final BytePipe bytePipe;

        public SubscriptionImpl(Subscriber<? super AudioStream> subscriber, BytePipe bytePipe)
        {
            this.subscriber = subscriber;
            this.bytePipe = bytePipe;
        }

        @Override
        public void request(long n)
        {
            if (n <= 0)
            {
                subscriber.onError(new IllegalArgumentException("Demand must be a positive number"));
            }

            demand.getAndAdd(n);

            executor.submit(() -> {
                try
                {
                    do
                    {
                        ByteBuffer audioBuffer = getNextAudioChunk();

                        if (audioBuffer.remaining() > 0)
                        {
                            AudioEvent audioEvent = audioEventFromBuffer(audioBuffer);
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

        private ByteBuffer getNextAudioChunk()
        {
            ByteBuffer audioBuffer;
            byte[] audioBytes = new byte[1024];
            int len = 0;

            try
            {
                len = bytePipe.read(audioBytes);

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
