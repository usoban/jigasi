package si.dlabs.jearni;

import org.jitsi.jigasi.transcription.Participant;
import org.jitsi.jigasi.transcription.Transcriber;

public class Utils
{
    public static String getCleanRoomName(Participant participant)
    {
        Transcriber transcriber = participant.getTranscriber();
        if (transcriber == null)
        {
            return "noroom";
        }

        String roomName = transcriber.getRoomName();
        int atSignIdx = roomName.indexOf('@');

        if (atSignIdx == -1)
        {
            return roomName;
        }
        else
        {
            return roomName.substring(0, atSignIdx);
        }
    }
}
