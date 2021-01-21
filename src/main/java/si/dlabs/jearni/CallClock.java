package si.dlabs.jearni;

import org.jitsi.utils.logging.Logger;
import software.amazon.awssdk.services.transcribestreaming.model.Result;
import software.amazon.awssdk.services.transcribestreaming.model.TranscriptEvent;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;

public class CallClock
{
    /**
     * Logger instance.
     */
    private final static Logger logger
            = Logger.getLogger(CallClock.class);

    private static HashMap<String, Instant> callStartDatetimes = new HashMap<>();

    public static void startCall(String callId)
    {
        callStartDatetimes.put(callId, Instant.now());
        logger.info("Call clock started for callId = " + callId);
    }

    public static void endCall(String callId)
    {
        callStartDatetimes.remove(callId);
        logger.info("Call clock ended for callId = " + callId);
    }

    public static TranscriptResult adjustRelativeToStartOfCall(
            String callId,
            TranscriptEvent transcriptEvent,
            Instant recognitionSessionStartDatetime,
            Duration recognitionSessionMutedDuration
    )
    {
        Instant callStartDatetime = callStartDatetimes.get(callId);
//        logger.info("Adjusting for callId = " + callId + " with start = " + callStartDatetime.toString());

        Duration callStartToSessionStartDelta = Duration.between(callStartDatetime, recognitionSessionStartDatetime);
        Duration totalAdjustmentDuration = callStartToSessionStartDelta.plus(recognitionSessionMutedDuration);

        Result r = transcriptEvent.transcript().results().get(0);
        TranscriptResult result = TranscriptResult.fromAmazonTranscriptResult(r);

        if (r.isPartial())
        {
            return null;
        }

        adjustTranscriptResult(result, totalAdjustmentDuration);

        return result;
    }

    private static void adjustTranscriptResult(TranscriptResult result, Duration forDuration)
    {
        double adjustment = forDuration.getSeconds();
        adjustment += 1e-9 * forDuration.getNano();

        result.startTime += adjustment;
        result.endTime += adjustment;

        for (TranscriptItem i : result.items)
        {
            i.startTime += adjustment;
            i.endTime += adjustment;
        }
    }
}
