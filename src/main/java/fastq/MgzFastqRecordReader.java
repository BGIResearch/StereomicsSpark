package fastq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.*;
import java.util.zip.GZIPInputStream;

import org.seqdoop.hadoop_bam.FormatConstants.BaseQualityEncoding;
import org.seqdoop.hadoop_bam.FormatConstants;
import org.seqdoop.hadoop_bam.FormatException;
import org.seqdoop.hadoop_bam.LineReader;
import org.seqdoop.hadoop_bam.SequencedFragment;

public class MgzFastqRecordReader extends RecordReader<Text, MGISequencedFragment>
{
    /*
     * fastq format:
     * <fastq> := <block>+
     * <block> := @<seqname>\n<seq>\n+[<seqname>]\n<qual>\n
     * <seqname> := [A-Za-z0-9_.:-/]+
     * <seq> := [A-Za-z\n\.~]+
     * <qual> := [!-~\n]+
     */

    //start: first valid data index
    private long start;
    //end: first index value beyond the slice, i.e. slice is in range [start,end)
    private long end;
    //pos: current position in file
    private long pos;
    //file: the file being read
    private Path file;

    private LineReader lineReader;
    private Text currentKey = new Text();
    private MGISequencedFragment currentValue = new MGISequencedFragment();

    private Text buffer = new Text();

    private BaseQualityEncoding qualityEncoding;

    //How long can a read get?
    private static final int MAX_LINE_LENGTH = 10000;

    public MgzFastqRecordReader(Configuration conf, FileSplit split) throws IOException {
        setConf(conf);
        file = split.getPath();
        start = split.getStart();
//        end  = start + split.getLength();
        long raw_size = getRawSize(conf, split.getLength());
        end = start + raw_size;

        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(file);
        fileIn.seek(start);
        GZIPInputStream gis = new GZIPInputStream(fileIn);

        pos = start;
        lineReader = new LineReader(gis);
    }

    public long getRawSize(Configuration conf, long length) throws IOException {
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(file);
        fileIn.seek(start);
        long size = 0;
        long raw_size = 0;
        while (size < length){
            fileIn.skip(12);
            byte[] data = new byte[4];
            fileIn.read(data);
            if ((data[0] & 0xff) != 73 || (data[1] & 0xff) != 71) {
                throw new RuntimeException("Invaild Indexed GZIP format, " + data[0] + " " + data[1]);
            }

            fileIn.read(data);
            int msize = getIntFromBytes(data[3], data[2], data[1], data[0]);
            size += msize;
            fileIn.skip(msize-24);
            fileIn.read(data);
            raw_size += getIntFromBytes(data[3], data[2], data[1], data[0]);
        }
        fileIn.close();
        return raw_size;
    }

    public static int getIntFromBytes(byte high_h, byte high_l, byte low_h, byte low_l) {
        return (high_h & 0xff) << 24 | (high_l & 0xff) << 16 | (low_h & 0xff) << 8 | low_l & 0xff;
    }

    protected void setConf(Configuration conf){
        String encoding =
                conf.get(MgzFastqInputFormat.CONF_BASE_QUALITY_ENCODING,
                        conf.get(FormatConstants.CONF_INPUT_BASE_QUALITY_ENCODING,
                                MgzFastqInputFormat.CONF_BASE_QUALITY_ENCODING_DEFAULT));
        if ("illumina".equals(encoding))
            qualityEncoding = BaseQualityEncoding.Illumina;
        else if ("sanger".equals(encoding))
            qualityEncoding = BaseQualityEncoding.Sanger;
        else
            throw new RuntimeException("Unknown input base quality encoding value " + encoding);
    }

    /**
     * Added to use mapreduce API.
     */
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
    }

    /**
     * Added to use mapreduce API.
     */
    public Text getCurrentKey()
    {
        return currentKey;
    }

    /**
     * Added to use mapreduce API.
     */
    public MGISequencedFragment getCurrentValue()
    {
        return currentValue;
    }

    /**
     * Added to use mapreduce API.
     */
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
        return next(currentKey, currentValue);
    }

    /**
     * Close this RecordReader to future operations.
     */
    public void close() throws IOException
    {
//        inputStream.close();
        lineReader.close();
    }

    /**
     * Create an object of the appropriate type to be used as a key.
     */
    public Text createKey()
    {
        return new Text();
    }

    /**
     * Create an object of the appropriate type to be used as a value.
     */
    public MGISequencedFragment createValue()
    {
        return new MGISequencedFragment();
    }

    /**
     * Returns the current position in the input.
     */
    public long getPos() { return pos; }

    /**
     * How much of the input has the RecordReader consumed i.e.
     */
    public float getProgress()
    {
        if (start == end)
            return 1.0f;
        else
            return Math.min(1.0f, (pos - start) / (float)(end - start));
    }

    public String makePositionMessage()
    {
        return file.toString() + ":" + pos;
    }

    protected boolean lowLevelFastqRead(Text key, MGISequencedFragment value) throws IOException
    {
        // ID line
        long shipped = lineReader.skip(1); // ship @
        pos += shipped;
        if (shipped == 0)
            return false; // EOF

        //ID
        readLineInto(key);
        // sequence
        value.clear();
        readLineInto(value.getSequence());
        readLineInto(buffer);
        if (buffer.getLength() == 0 || buffer.getBytes()[0] != '+')
            throw new RuntimeException("unexpected fastq line separating sequence and quality at " + makePositionMessage() + ". Line: " + buffer + ". \nSequence ID: " + key);
        readLineInto(value.getQuality());
        scanNameForReadNumber(key, value);
        return true;
    }

    public boolean next(Text key, MGISequencedFragment value) throws IOException
    {
        if (pos >= end)
            return false; //past end of slice
        try
        {
            boolean gotData;
            boolean goodRecord;
            do {
                gotData = lowLevelFastqRead(key, value);
                goodRecord = gotData;
            }while (gotData && !goodRecord);

            if (goodRecord) // goodRecord falso also when we couldn't read any more data
            {
                if (qualityEncoding == BaseQualityEncoding.Illumina)
                {
                    try
                    {
                        // convert illumina to sanger scale
                        SequencedFragment.convertQuality(value.getQuality(), BaseQualityEncoding.Illumina, BaseQualityEncoding.Sanger);
                    } catch (FormatException e) {
                        throw new FormatException(e.getMessage() + " Position: " + makePositionMessage() + "; Sequence ID: " + key);
                    }
                }
                else // sanger qualities.
                {
                    int outOfRangeElement = SequencedFragment.verifyQuality(value.getQuality(), BaseQualityEncoding.Sanger);
                    if (outOfRangeElement >= 0)
                    {
                        throw new FormatException("fastq base quality score out of range for Sanger Phred+33 format (found " +
                                (value.getQuality().getBytes()[outOfRangeElement] - FormatConstants.SANGER_OFFSET) + ").\n" +
                                "Although Sanger format has been requested, maybe qualities are in Illumina Phred+64 format?\n" +
                                "Position: " + makePositionMessage() + "; Sequence ID: " + key);
                    }
                }
            }
            return goodRecord;
        }
        catch (EOFException e) {
            throw new RuntimeException("unexpected end of file in fastq record at " + makePositionMessage() + ".  Id: " + key.toString());
        }
    }
    private void scanNameForReadNumber(Text name, MGISequencedFragment fragment)
    {
        // look for a /[0-9] at the end of the name
        if (name.getLength() >= 2)
        {
            byte[] bytes = name.getBytes();
            int last = name.getLength() - 1;

            if (bytes[last-1] == '/' && bytes[last] >= '0' && bytes[last] <= '9')
                name.set(bytes, 0, last-1);
            fragment.setRead(bytes[last] - '0');
        }
    }
    private int readLineInto(Text dest) throws EOFException, IOException
    {
        int bytesRead = lineReader.readLine(dest, MAX_LINE_LENGTH);
        if (bytesRead <= 0)
            throw new EOFException();
        pos += bytesRead;
        return bytesRead;
    }
}
