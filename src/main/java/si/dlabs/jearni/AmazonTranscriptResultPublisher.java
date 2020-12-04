package si.dlabs.jearni;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.jitsi.jigasi.transcription.*;
import org.jitsi.utils.logging.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import software.amazon.awssdk.services.transcribestreaming.model.*;
import software.amazon.awssdk.services.transcribestreaming.model.TranscriptEvent;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;

// TODO: Remove TranscriptionListener interface - or swap with TranscriptionEventListener interface?

public class AmazonTranscriptResultPublisher
    implements TranscriptionListener
{
    private final static Logger logger
            = Logger.getLogger(AmazonTranscriptResultPublisher.class);

    private Participant participant;

    private Connection mqConnection;

    private Channel transcriptChannel;

    private String exchangeName = "speech-transcription";

    private String routingKey = "test";

    public AmazonTranscriptResultPublisher(Participant participant)
    {
        this.participant = participant;

        try
        {
            mqConnection = RabbitMQConnectionFactory.getConnection();
            transcriptChannel = mqConnection.createChannel();
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

        transcriptChannel.exchangeDeclare(exchangeName, "fanout", true);
        transcriptChannel.queueDeclare(queueName, true, false, false, null);
        transcriptChannel.queueBind(queueName, exchangeName, routingKey);
    }

    public void publish(TranscriptEvent transcriptEvent)
    {
        // TODO.
        // 1. extract
        Result result = transcriptEvent.transcript().results().get(0);

        if (result.isPartial())
        {
            return;
        }

        publishRawTranscriptResult(result);

        Alternative firstAlternative = result.alternatives().get(0);

        // TODO: send transcript 'payload' data structure
    }

    private List<Sentence> breakAlternativeIntoSentences(Alternative transcriptAlternative)
    {
        List<Sentence> sentences = new LinkedList<>();
        Sentence currentSentence = new Sentence();

        Item[] items = (Item[])transcriptAlternative.items().toArray();

        for (Item item : items)
        {
            if (item.type().equals(ItemType.PUNCTUATION))
            {
                String content = item.content();

                switch (content) {
                    case ".":
                    case "!":
                        currentSentence.setType(Sentence.SentenceType.NON_QUESTION);
                        sentences.add(currentSentence);
                        currentSentence = new Sentence();
                        break;

                    case "?":
                        currentSentence.setType(Sentence.SentenceType.QUESTION);
                        sentences.add(currentSentence);
                        currentSentence = new Sentence();
                        break;

                    case ",":
                        currentSentence.addComma();
                        break;

                    default:
                        // some other type of pause (as per Amazon's developer guide,
                        // punctuations can be any pauses in the speech
                        logger.warn("An unknown punctuation type spotted: " + content);
                        break;
                }
            }
            else if (item.type().equals(ItemType.PRONUNCIATION))
            {
                currentSentence.addUtterance(item.content());
            }
            else
            {
                logger.warn("An unknown item type spotted: " + item.typeAsString());
            }
        }

        if (!currentSentence.isEmpty())
        {
            // Could it happen that alternative ends without a punctuation?
            sentences.add(currentSentence);
            logger.warn("Transcript alternative ended without a punctuation; still adding to list of sentences.");
        }

        return sentences;
    }

    /**
     * Publishes a raw Amazon's transcribe streaming result
     *
     * @param transcriptResult
     */
    private void publishRawTranscriptResult(Result transcriptResult)
    {
        JSONObject resultRawJson = resultToRawJson(transcriptResult);

        // TODO: publish :)
    }

    /**
     * Converts Amazon Streaming Transcription result to its JSON form.
     *
     * {@link https://docs.aws.amazon.com/transcribe/latest/dg/API_streaming_Result.html}
     *
     * @param transcriptResult Amazon Streaming Transcription result object
     * @return JSON form
     */
    private JSONObject resultToRawJson(Result transcriptResult)
    {
        JSONObject json = new JSONObject();

        LinkedList<JSONObject> alternativesJson = new LinkedList<>();
        transcriptResult.alternatives().forEach(alt -> alternativesJson.add(alternativeToRawJson(alt)));

        json.put("ResultId", transcriptResult.resultId());
        json.put("IsPartial", transcriptResult.isPartial());
        json.put("StartTime", transcriptResult.startTime());
        json.put("EndTime", transcriptResult.endTime());
        json.put("Alternatives", new JSONArray(alternativesJson));

        return json;
    }

    /**
     * Converts Amazon Streaming Transcription result's alternative to its JSON form.
     *
     * {@link https://docs.aws.amazon.com/transcribe/latest/dg/API_streaming_Alternative.html}
     *
     * @param transcriptResultAlternative Amazon Streaming Transcription Result Alternative
     * @return JSON form
     */
    private JSONObject alternativeToRawJson(Alternative transcriptResultAlternative)
    {
        JSONObject json = new JSONObject();

        LinkedList<JSONObject> itemsJson = new LinkedList<>();
        transcriptResultAlternative.items().forEach(item -> {
            JSONObject itemJson = new JSONObject();

            itemJson.put("StartTime", item.startTime());
            itemJson.put("EndTime", item.endTime());
            itemJson.put("Content", item.content());
            itemJson.put("Type", item.typeAsString());
            itemJson.put("VocabularyFilterMatch", item.vocabularyFilterMatch());

            itemsJson.add(itemJson);
        });

        json.put("Transcript", transcriptResultAlternative.transcript());
        json.put("Items", new JSONArray(itemsJson));


        return json;
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
            transcriptChannel.basicPublish(
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
