package si.dlabs.jearni;

import java.util.LinkedList;
import java.util.List;

public class Sentence
{
    public enum SentenceType
    {
        QUESTION, NON_QUESTION
    }

    private StringBuilder content = new StringBuilder();

    private List<String> tokens = new LinkedList<>();

    private SentenceType type;

    // TODO: check if utterance is good term for words and other stuff such as "Umm", "Mmm", ...
    public void addUtterance(String utterance)
    {
        tokens.add(utterance);
    }

    public void addComma()
    {
        tokens.add(",");
    }

    public void setType(SentenceType type)
    {
        this.type = type;
    }

    public String getContent()
    {
        StringBuilder builder = new StringBuilder();
        boolean start = true;

        for (String token : tokens) {
            // Add a space if:
            //  1. we're not at the beginning of the sentence
            //  2. current token is not a comma
            if (!start && !token.equals(","))
            {
                builder.append(" ");
            }
            if (start)
            {
                start = false;
            }

            builder.append(token);
        }

        switch (type)
        {
            case QUESTION:
                builder.append("?");
                break;

            case NON_QUESTION:
                builder.append(".");
                break;
        }

        return builder.toString();
    }

    public boolean isEmpty()
    {
        return tokens.size() == 0;
    }
}
