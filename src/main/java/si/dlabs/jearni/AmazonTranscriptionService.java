package si.dlabs.jearni;

import org.jitsi.jigasi.JigasiBundleActivator;
import org.jitsi.jigasi.transcription.*;
import org.jitsi.service.configuration.ConfigurationService;
import org.jitsi.utils.logging.Logger;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.transcribestreaming.TranscribeStreamingAsyncClient;
import software.amazon.awssdk.services.transcribestreaming.model.*;
import software.amazon.awssdk.services.transcribestreaming.model.TranscriptEvent;
import javax.media.format.AudioFormat;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class AmazonTranscriptionService
    implements TranscriptionService
{
    private static String PROP_BUFFER_MILLISECONDS = "org.jitsi.jigasi.transcription.AMAZON_BUFFER_MILLISECONDS";
    private static int PROP_BUFFER_MILLISECONDS_DEFAULT = 500;

    private static String PROP_TRANSCRIPTION_LANGUAGE = "org.jitsi.jigasi.transcription.AMAZON_TRANSCRIBE_LANGUAGE";
    private static String PROP_TRANSCRIPTION_LANGUAGE_DEFAULT = LanguageCode.EN_US.toString();

    /**
     * Logger instance.
     */
    private final static Logger logger
            = Logger.getLogger(AmazonTranscriptionService.class);

    /**
     * Amazon transcribing client.
     */
    private TranscribeStreamingAsyncClient client;

    /**
     * The number of milliseconds we want to buffer the audio before sending it to transcription service.
     */
    private int bufferSizeMilliseconds;

    /**
     * Computed number of bytes required to satisfy the buffer length in milliseconds (depends on audio format).
     */
    private int bufferSize;

    /**
     * The language code used for Amazon's Transcribe.
     */
    private String transcribeLanguage;

    @SuppressWarnings("WeakerAccess")
    public AmazonTranscriptionService()
    {
        client = TranscribeStreamingAsyncClient
                .builder()
                .credentialsProvider(EnvironmentVariableCredentialsProvider.create())
                .build();

        configure();
    }

    private void configure()
    {
        ConfigurationService configurationService = JigasiBundleActivator.getConfigurationService();
        if (configurationService != null)
        {

            bufferSizeMilliseconds = configurationService.getInt(
                    PROP_BUFFER_MILLISECONDS,
                    PROP_BUFFER_MILLISECONDS_DEFAULT
            );

            transcribeLanguage = configurationService.getString(
                    PROP_TRANSCRIPTION_LANGUAGE,
                    PROP_TRANSCRIPTION_LANGUAGE_DEFAULT
            );
        }
        else
        {
            bufferSizeMilliseconds = PROP_BUFFER_MILLISECONDS_DEFAULT;
            transcribeLanguage = PROP_TRANSCRIPTION_LANGUAGE_DEFAULT;
        }

        logger.info("Amazon Transcribe buffer = " + bufferSizeMilliseconds + "ms.");
    }

    @Override
    public boolean supportsFragmentTranscription()
    {
        return true;
    }

    @Override
    public void sendSingleRequest(TranscriptionRequest request, Consumer<TranscriptionResult> resultConsumer)
            throws UnsupportedOperationException
    {
        logger.error("sendSingleRequest is not yet implemented.");
    }

    @Override
    public boolean supportsStreamRecognition()
    {
        return true;
    }

    @Override
    public StreamingRecognitionSession initStreamingSession(Participant participant)
            throws UnsupportedOperationException
    {
        return new AmazonStreamingRecognitionSession(participant, client);
    }

    @Override
    public boolean isConfiguredProperly()
    {
        return true;
    }

    public class AmazonStreamingRecognitionSession implements StreamingRecognitionSession
    {
        private final TranscribeStreamingAsyncClient client;
        private AmazonAudioStreamPublisher audioPublisher;
        private CompletableFuture<Void> streamTranscriptionFuture;
        private List<TranscriptionListener> transcriptionListeners = new LinkedList<TranscriptionListener>();
        private UUID messageId;
        private Participant participant;
        private AmazonTranscriptResultPublisher resultPublisher;
        private final AtomicBoolean isSessionActive = new AtomicBoolean(true);
        private final AtomicBoolean isTranscriptionStreamingRequestRunning = new AtomicBoolean(false);

        /**
         * The instant when we received Amazon's response that the recognition session has started.
         */
        private Instant recognitionSessionStartDatetime;

        public AmazonStreamingRecognitionSession(Participant participant, TranscribeStreamingAsyncClient client)
        {
            this.participant = participant;
            this.client = client;
            messageId = UUID.randomUUID();

            resultPublisher = new AmazonTranscriptResultPublisher(participant);

            logger.info("Amazon streaming recognition session initialized for participant " + participant.getId());
        }

        protected StartStreamTranscriptionRequest buildStreamingRequest(TranscriptionRequest request)
        {
            AudioFormat audioFormat = request.getFormat();

            logger.info("Audio encoding is " + audioFormat.toString());

            if (
                    !audioFormat.getEncoding().equals(AudioFormat.LINEAR) &&
                    !audioFormat.getEncoding().equals("PCM_SIGNED") // Mic testing
            )
            {
                throw new IllegalArgumentException("Audio format encoding not supported: " + audioFormat.toString());
            }

            int sampleRateInHertz = Double.valueOf(audioFormat.getSampleRate()).intValue();

            logger.info("Language configured for transcription: " + transcribeLanguage);

            StartStreamTranscriptionRequest.Builder builder =
                    StartStreamTranscriptionRequest
                    .builder()
                    .mediaEncoding(MediaEncoding.PCM)
                    .languageCode(transcribeLanguage)
                    .mediaSampleRateHertz(sampleRateInHertz);

            if (transcribeLanguage.equals(PROP_TRANSCRIPTION_LANGUAGE_DEFAULT))
            {
                builder.vocabularyName("filler-words-"+transcribeLanguage);
            }

            return builder.build();
        }

        protected StartStreamTranscriptionResponseHandler buildStreamingResponseHandler()
        {
            return StartStreamTranscriptionResponseHandler.builder()
                    .onResponse(r -> {
                        recognitionSessionStartDatetime = Instant.now();
                        logger.info("Received intial response for participant " + participant.getId());
                        logger.info(r);
                    })
                    .onError(e -> {
                        logger.error("ResponseHandler error occured for participant " + participant.getId(), e);
                        isTranscriptionStreamingRequestRunning.set(false);
                    })
                    .onComplete(() -> {
                        logger.info("All records streamed successfully for participant " + participant.getId());
                        isTranscriptionStreamingRequestRunning.set(false);
                    })
                    .subscriber(buildResponseHandler())
                    .build();
        }

        protected Consumer<TranscriptResultStream> buildResponseHandler()
        {
            return (TranscriptResultStream e) -> {
                TranscriptEvent event = (TranscriptEvent) e;

                if (logger.isDebugEnabled())
                {
                    logger.debug(event.toString());
                }

                if (event.transcript().results().size() < 1)
                {
                    return;
                }

                while (recognitionSessionStartDatetime == null)
                {
                    // just wait....
                    logger.warn("Recognition session start datetime still not set, waiting...");
                }

                String conferenceId = Utils.getCleanRoomName(participant);
                TranscriptResult adjustedTranscriptResult = CallClock.adjustRelativeToStartOfCall(
                        conferenceId,
                        event,
                        recognitionSessionStartDatetime,
                        audioPublisher.getCurrentSubscriptionMutedDuration()
                );

                if (adjustedTranscriptResult == null)
                {
                    return;
                }

                resultPublisher.publish(event, adjustedTranscriptResult);

//                Result firstResult = event.transcript().results().get(0);
//
//                if (firstResult.alternatives().size() < 1)
//                {
//                    logger.warn("Transcription result has no alternatives?");
//                }
//
//                Alternative firstAlternative = firstResult.alternatives().get(0);
//                TranscriptionAlternative transcriptionAlternative = new TranscriptionAlternative(
//                        firstAlternative.transcript()
//                );
//                TranscriptionResult transcriptionResult = new TranscriptionResult(
//                        null,
//                        messageId,
//                        firstResult.isPartial(),
//                        transcribeLanguage,
//                        1.0,
//                        transcriptionAlternative
//                );
//
//                for (TranscriptionListener listener : transcriptionListeners)
//                {
//                    listener.notify(transcriptionResult);
//                }
            };
        }

        protected synchronized void startStreamingRequest(TranscriptionRequest transcriptionRequest) throws IOException
        {
            boolean isRunning = isTranscriptionStreamingRequestRunning.getAndSet(true);
            if (isRunning)
            {
                return;
            }

            if (audioPublisher == null)
            {
                // Set up buffers and the publisher.
                AudioFormat audioFormat = transcriptionRequest.getFormat();
                int samplesPerMs = new Double(audioFormat.getSampleRate() / 1000.0).intValue(); // number of samples per millisecond.
                int bytesPerSample = audioFormat.getSampleSizeInBits() / 8;
                bufferSize = bufferSizeMilliseconds * samplesPerMs * bytesPerSample;

                logger.info("Buffer size for " + bufferSizeMilliseconds + "ms is " + bufferSize + " bytes.");

                audioPublisher = new AmazonAudioStreamPublisher(bufferSize);
            }

            StartStreamTranscriptionRequest request = buildStreamingRequest(transcriptionRequest);
            StartStreamTranscriptionResponseHandler responseHandler = buildStreamingResponseHandler();


            logger.info("Starting streaming transcription for participant " + participant.getId());

            streamTranscriptionFuture = client.startStreamTranscription(request, audioPublisher, responseHandler);
            isTranscriptionStreamingRequestRunning.set(true);

            logger.info("Streaming transcription started for participant " + participant.getId());
        }

        @Override
        public void sendRequest(TranscriptionRequest request)
        {
            if (!isTranscriptionStreamingRequestRunning.get())
            {
                try
                {
                    startStreamingRequest(request);
                }
                catch (IOException e)
                {
                    logger.error("Error starting streaming request for participant " + participant.getId(), e);
                }
            }

            audioPublisher.pushAudioBytes(request.getAudio());
        }

        @Override
        public void end()
        {
            logger.info("Ending AmazonStreamingRecognitionSession for participant " + participant.getId());
            isSessionActive.set(false);

            if (isTranscriptionStreamingRequestRunning.get() && streamTranscriptionFuture != null)
            {
                streamTranscriptionFuture.cancel(true);
            }

            this.client.close();
            this.resultPublisher.end();

            logger.info("AmazonStreamingRecognitionSession ended for participant " + participant.getId());
        }

        @Override
        public boolean ended()
        {
            return !isSessionActive.get();
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
        private SubscriptionImpl currentSubscription;

        public AmazonAudioStreamPublisher(int bufferSize)
                throws IOException
        {
            bytePipe = new BytePipe(bufferSize);
        }

        @Override
        public void subscribe(Subscriber<? super AudioStream> subscriber)
        {
            currentSubscription = new SubscriptionImpl(subscriber, bytePipe);
            subscriber.onSubscribe(currentSubscription);
        }

        public Duration getCurrentSubscriptionMutedDuration()
        {
            return currentSubscription.mutedDuration;
        }

        public void pushAudioBytes(byte[] bytes)
        {
            try
            {
                bytePipe.write(bytes);
            }
            catch (IOException e)
            {
                logger.error("Error pushing bytes", e);
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

        private Instant lastAudioPacketSentAt;
        private Duration mutedDuration = Duration.ofSeconds(0, 0);

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
                Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
                try
                {
                    do
                    {
                        ByteBuffer audioBuffer = getNextAudioChunk();

                        if (audioBuffer.remaining() > 0)
                        {
                            AudioEvent audioEvent = audioEventFromBuffer(audioBuffer);

                            Instant now = Instant.now();
                            if (lastAudioPacketSentAt != null)
                            {
                                Duration delta = Duration.between(lastAudioPacketSentAt, now);

                                if (delta.getSeconds() > 2)
                                {
                                    // TODO: participant has likely muted his microphone. This is a strong assumption,
                                    //       but we can't do better at this point :(

                                    mutedDuration = mutedDuration.plusSeconds(delta.getSeconds());
                                    mutedDuration = mutedDuration.plusNanos(delta.getNano());
                                }
                            }
                            lastAudioPacketSentAt = now;

                            subscriber.onNext(audioEvent);
                        }
                        else
                        {
                            logger.info("Completed subscription task.");
                            subscriber.onComplete();
                            break;
                        }
                    } while(demand.decrementAndGet() > 0);
                }
                catch (Exception e)
                {
                    logger.error("Subscription error occurred.", e);
                    subscriber.onError(e);
                }
            });
        }

        @Override
        public void cancel()
        {
            logger.info("Canceling audio data subscription.");
            executor.shutdownNow();

            try
            {
                bytePipe.reset();
            }
            catch (IOException e)
            {
                logger.error("Error resetting byte-pipe", e);
            }


            if (subscriber != null)
            {
                subscriber.onComplete();
            }
        }

        private ByteBuffer getNextAudioChunk()
        {
            try
            {
                byte[] audioBytes = new byte[bufferSize];
                int len = bytePipe.read(audioBytes);

                if (len == 0)
                {
                    return ByteBuffer.allocate(0);
                }

                return ByteBuffer.wrap(audioBytes, 0, len);
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
