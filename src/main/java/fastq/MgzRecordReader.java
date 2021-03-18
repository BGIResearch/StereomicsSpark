package fastq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MgzRecordReader extends RecordReader<Text, BytesWritable>
{
    //start: first valid data index
    private long start;
    //end: first index value beyond the slice, i.e. slice is in range [start,end)
    private long end;

    //file: the file being read
    private Path file;

    private Text currentKey = new Text();
    private BytesWritable currentValue = new BytesWritable();


    public MgzRecordReader(Configuration conf, FileSplit split) throws IOException {
        file = split.getPath();
        start = split.getStart();
        end  = start + split.getLength();

        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(file);

        int length = (int)split.getLength();

        // compressed file
        byte[] data = new byte[length];
        fileIn.seek(start);
        fileIn.read(data, 0, length);
        currentValue.set(data, 0, length);
    }


    /**
     * Added to use mapreduce API.
     */
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(start < end){
            start = end;
            return true;
        }
        return false;
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
    public BytesWritable getCurrentValue()
    {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

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
    public BytesWritable createValue()
    {
        return new BytesWritable();
    }

}
