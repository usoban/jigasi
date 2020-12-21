package si.dlabs.jearni;

import org.jitsi.jigasi.transcription.Participant;

public class Utils
{
    public static String getCleanRoomName(Participant participant)
    {
        String roomName = participant.getTranscriber().getRoomName();
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
