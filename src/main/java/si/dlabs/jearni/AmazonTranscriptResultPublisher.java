package si.dlabs.jearni;

import com.rabbitmq.client.*;
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

public class AmazonTranscriptResultPublisher
{
    private static final String RAW_TRANSCRIPT_EXCHANGE_NAME = "raw-transcript";

    private static final String TRANSCRIPT_EXCHANGE_NAME = "transcript";

    private final static Logger logger
            = Logger.getLogger(AmazonTranscriptResultPublisher.class);

    private final Participant participant;

    private Connection mqConnection;

    private Channel transcriptChannel;

    public AmazonTranscriptResultPublisher(Participant participant)
    {
        this.participant = participant;

        try
        {
            configureMq();
        }
        catch (IOException | TimeoutException e)
        {
            logger.error("Error establishing connection to message exchange", e);
        }
    }

    private void configureMq()
            throws IOException, TimeoutException
    {
        mqConnection = RabbitMQConnectionFactory.getConnection();
        transcriptChannel = mqConnection.createChannel();

        transcriptChannel.exchangeDeclarePassive(RAW_TRANSCRIPT_EXCHANGE_NAME);
        transcriptChannel.exchangeDeclarePassive(TRANSCRIPT_EXCHANGE_NAME);
    }

    public void publish(TranscriptEvent transcriptEvent)
    {
        Result result = transcriptEvent.transcript().results().get(0);

        if (result.isPartial())
        {
            return;
        }

        publishRawTranscriptResult(result);

        Alternative firstAlternative = result.alternatives().get(0);

        List<Sentence> sentences = breakAlternativeIntoSentences(firstAlternative);

        for (Sentence s : sentences)
        {
            logger.info("[Participant " + participant.getId() + "] Sentence: " + s.getContent());
            publishSentence(s);
        }
    }

    private List<Sentence> breakAlternativeIntoSentences(Alternative transcriptAlternative)
    {
        List<Sentence> sentences = new LinkedList<>();
        Sentence currentSentence = null;

        List<Item> items = transcriptAlternative.items();

        for (Item item : items)
        {
            if (currentSentence == null)
            {
                currentSentence = new Sentence(item.startTime());
            }

            if (item.type().equals(ItemType.PUNCTUATION))
            {
                String content = item.content();

                switch (content) {
                    case ".":
                    case "!":
                        currentSentence.finish(Sentence.SentenceType.NON_QUESTION, item.endTime());
                        sentences.add(currentSentence);
                        currentSentence = null;
                        break;

                    case "?":
                        currentSentence.finish(Sentence.SentenceType.QUESTION, item.endTime());
                        sentences.add(currentSentence);
                        currentSentence = null;
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

        if (currentSentence != null && currentSentence.isEmpty())
        {
            // Could it happen that alternative ends without a punctuation?
            sentences.add(currentSentence);
            logger.warn("Transcript alternative ended without a punctuation; still adding to list of sentences.");
        }

        return sentences;
    }

    private void publishSentence(Sentence sentence)
    {
        String conferenceId = Utils.getCleanRoomName(participant);
        JSONObject json = new JSONObject();

        json.put("conversation_id", conferenceId);
        json.put("speaker_id", participant.getId());
        json.put("start_time", sentence.getStartTime());
        json.put("end_time", sentence.getEndTime());
        json.put("text", sentence.getContent());
        json.put("word_count", sentence.getWordCount());
        json.put("sentence_type", sentence.getTypeString());
        json.put("sentence_speaking_time",  sentence.getEndTime() - sentence.getStartTime());

        String stringJson = json.toString();
        if (stringJson == null)
        {
            logger.error("Something went wrong while transforming JSON object to its string representation.");
            return;
        }

        try
        {
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .priority(1)
                    .build();

            transcriptChannel.basicPublish(TRANSCRIPT_EXCHANGE_NAME, conferenceId, properties, stringJson.getBytes());
        }
        catch (IOException e)
        {
            logger.error("Error publishing transcript to exchange", e);
        }
    }

    /**
     * Publishes a raw Amazon's transcribe streaming result
     *
     * @param transcriptResult
     */
    private void publishRawTranscriptResult(Result transcriptResult)
    {
        String conferenceId = Utils.getCleanRoomName(participant);
        JSONObject resultRawJson = resultToRawJson(transcriptResult);
        String stringJson = resultRawJson.toString();

        if (stringJson == null)
        {
            logger.error("Something went wrong transforming raw transcript to its string representation.");
            return;
        }

        try
        {
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .contentType("application/json")
                    .deliveryMode(2)
                    .priority(1)
                    .build();

            transcriptChannel.basicPublish(
                    RAW_TRANSCRIPT_EXCHANGE_NAME,
                    conferenceId,
                    properties,
                    stringJson.getBytes()
            );
        }
        catch (IOException e)
        {
            logger.error("Error publishing raw transcript to exchange", e);
        }
    }

    /**
     * Converts Amazon Streaming Transcription result to its JSON form.
     *
     * @see <a href="https://docs.aws.amazon.com/transcribe/latest/dg/API_streaming_Result.html">API Streaming Result</a>
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
     * @see <a href="https://docs.aws.amazon.com/transcribe/latest/dg/API_streaming_Alternative.html">API Streaming Alternative</a>
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

    public void end()
    {
        if (transcriptChannel != null)
        {
            try {
                transcriptChannel.close();
            } catch (IOException | TimeoutException e) {
                logger.error("Error closing AmazonTranscriptResultPublisher's MQ channel.", e);
            }
        }

        if (mqConnection != null)
        {
            mqConnection = null;
            RabbitMQConnectionFactory.releaseConnection();
        }

        logger.info("AmazonTranscriptionResultPublisher for participant [" + participant.getId() + "] ended." );
    }
}
