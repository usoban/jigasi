package si.dlabs.jearni;

import software.amazon.awssdk.services.transcribestreaming.model.Alternative;
import software.amazon.awssdk.services.transcribestreaming.model.Result;
import java.util.LinkedList;
import java.util.List;

public class TranscriptResult
{
    public double startTime;
    public double endTime;
    public List<TranscriptItem> items;

    public static TranscriptResult fromAmazonTranscriptResult(Result result)
    {
        TranscriptResult r = new TranscriptResult();

        r.startTime = result.startTime();
        r.endTime = result.endTime();

        Alternative a = result.alternatives().get(0);

        r.items = new LinkedList<>();
        for (software.amazon.awssdk.services.transcribestreaming.model.Item item : a.items())
        {
            r.items.add(TranscriptItem.fromAmazonTranscriptAlternativeItem(item));
        }

        return r;
    }
}
