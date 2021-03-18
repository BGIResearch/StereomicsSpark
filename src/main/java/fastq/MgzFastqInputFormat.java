package fastq;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.StopWatch;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MgzFastqInputFormat extends FileInputFormat<Text, MGISequencedFragment>{

    public static final int SPLIT_BLOCK_NUM = 10;
    public static final String CONF_BASE_QUALITY_ENCODING = "hbam.fastq-input.base-quality-encoding";
    public static final String CONF_FILTER_FAILED_QC      = "hbam.fastq-input.filter-failed-qc";
    public static final String CONF_BASE_QUALITY_ENCODING_DEFAULT = "sanger";

    private static final Log LOG = LogFactory.getLog(FileInputFormat.class);


    @Override
    public RecordReader<Text, MGISequencedFragment> createRecordReader(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        context.setStatus(genericSplit.toString());
        return new MgzFastqRecordReader(context.getConfiguration(), (FileSplit)genericSplit);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file){
        if (file.toString().endsWith(".gz")){
            try {
                if(file.getFileSystem(context.getConfiguration()).exists(file.suffix(".idx")))
                    return true;
                else {
                    LOG.error("mgz index file does not exist");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return false;

        }
        return true;
    }

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        StopWatch sw = (new StopWatch()).start();
        long minSize = Math.max(this.getFormatMinSplitSize(), getMinSplitSize(job));
        List<InputSplit> splits = new ArrayList();
        List<FileStatus> files = this.listStatus(job);
        Iterator var9 = files.iterator();

        while(var9.hasNext()) {
            FileStatus file = (FileStatus)var9.next();
            Path path = file.getPath();

            long length = file.getLen();
            if (length != 0L) {
                FileSystem fs = path.getFileSystem(job.getConfiguration());
                BlockLocation[] blkLocations = fs.getFileBlockLocations(file, 0L, length);

                if (this.isSplitable(job, path)) {
                    FSDataInputStream FSinput = fs.open(path.suffix(".idx"));

                    LineReader lineReader = new LineReader(FSinput, job.getConfiguration());
                    Text line = new Text();
                    long bytesRemaining = length;
                    int blkIndex;
                    int block_num = 0;
                    long splitSize = 0;
                    long start = 0;
                    while ((lineReader.readLine(line)) != 0) {
                        if(line.toString().startsWith("#"))
                            continue;
                        String[] fields = line.toString().split("\t");

                        if(block_num == 0)
                            start = Long.parseLong(fields[1]);
                        splitSize += Long.parseLong(fields[2]);

                        block_num++;

                        if(block_num>=SPLIT_BLOCK_NUM){
                            blkIndex = this.getBlockIndex(blkLocations, length - bytesRemaining);
                            splits.add(this.makeSplit(path, start, splitSize, blkLocations[blkIndex].getHosts()));
                            bytesRemaining -= splitSize;
                            splitSize = 0;
                            block_num = 0;
                        }
                    }

                    if(block_num>0){
                        blkIndex = this.getBlockIndex(blkLocations, length - bytesRemaining);
                        splits.add(this.makeSplit(path, start, splitSize, blkLocations[blkIndex].getHosts()));
                    }
                    lineReader.close();
                } else {
                    if (LOG.isDebugEnabled() && length > Math.min(file.getBlockSize(), minSize)) {
                        LOG.debug("File is not splittable so no parallelization is possible: " + file.getPath());
                    }

                    splits.add(this.makeSplit(path, 0L, length, blkLocations[0].getHosts(), blkLocations[0].getCachedHosts()));
                }
            } else {
                splits.add(this.makeSplit(path, 0L, length, new String[0]));
            }
        }

        job.getConfiguration().setLong("mapreduce.input.fileinputformat.numinputfiles", (long)files.size());
        sw.stop();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Total # of splits generated by getSplits: " + splits.size() + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
        }

        return splits;
    }

}