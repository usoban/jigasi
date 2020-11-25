package si.dlabs.jearni;

import java.io.*;

public class BytePipe
{
    private final PipedInputStream inputPipe;
    private final PipedOutputStream outputPipe;

    public BytePipe() throws IOException
    {
        this(1024);
    }

    public BytePipe(int bufferSize) throws IOException
    {
        inputPipe = new PipedInputStream(bufferSize);
        outputPipe = new PipedOutputStream(inputPipe);
    }

    public void write(byte[] bytes)
            throws IOException
    {
        for (byte b : bytes) {
            outputPipe.write(b);
        }
    }

    public int read(byte[] buffer)
            throws IOException
    {
        return inputPipe.read(buffer);
    }

    public int available()
            throws IOException
    {
        return inputPipe.available();
    }
}