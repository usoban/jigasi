package si.dlabs.jearni;

import software.amazon.awssdk.services.transcribestreaming.model.Item;
import software.amazon.awssdk.services.transcribestreaming.model.ItemType;

public class TranscriptItem
{
    public double startTime;
    public double endTime;
    public String content;
    public ItemType type;

    public static TranscriptItem fromAmazonTranscriptAlternativeItem(Item item)
    {
        TranscriptItem i = new TranscriptItem();

        i.startTime = item.startTime();
        i.endTime = item.endTime();
        i.content = item.content();
        i.type = item.type();

        return i;
    }

    public String typeAsString()
    {
        return type.toString();
    }
}
