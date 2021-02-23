package fastq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.InputStream;
import java.io.IOException;
import java.io.EOFException;

import java.util.regex.*;

import org.seqdoop.hadoop_bam.FormatConstants.BaseQualityEncoding;
import org.seqdoop.hadoop_bam.FormatConstants;
import org.seqdoop.hadoop_bam.FormatException;
import org.seqdoop.hadoop_bam.LineReader;
import org.seqdoop.hadoop_bam.SequencedFragment;
import org.seqdoop.hadoop_bam.util.ConfHelper;

public class MGIFastqInputFormat extends FileInputFormat<Text, MGISequencedFragment>{

	public static final String CONF_BASE_QUALITY_ENCODING = "hbam.fastq-input.base-quality-encoding";
	public static final String CONF_FILTER_FAILED_QC      = "hbam.fastq-input.filter-failed-qc";
	public static final String CONF_BASE_QUALITY_ENCODING_DEFAULT = "sanger";
	
	public static class FastqRecordReader extends RecordReader<Text, MGISequencedFragment>
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
		private InputStream inputStream;
		private Text currentKey = new Text();
		private MGISequencedFragment currentValue = new MGISequencedFragment();
		
		private Text buffer = new Text();
		
		private BaseQualityEncoding qualityEncoding;
		
		//How long can a read get?
		private static final int MAX_LINE_LENGTH = 10000;
		
		public FastqRecordReader(Configuration conf, FileSplit split) throws IOException
		{
			setConf(conf);
			file = split.getPath();
			start = split.getStart();
			end  = start + split.getLength();
			
			FileSystem fs = file.getFileSystem(conf);
			FSDataInputStream fileIn = fs.open(file);
			
			CompressionCodecFactory codecFactory = new CompressionCodecFactory(conf);
			CompressionCodec        codec        = codecFactory.getCodec(file);
			
			if (codec == null) // no codec. Uncompressed file.
			{
				positionAtFirstRecord(fileIn);
				inputStream = fileIn;
			}
			else
			{
				// compressed file
				if (start != 0)
					throw new RuntimeException("Start position for compressed file is not 0! (found " + start + ")");
				inputStream = codec.createInputStream(fileIn);
				end = Long.MAX_VALUE; // read until the end of th file
			}
			lineReader = new LineReader(inputStream);
		}
		
		protected void setConf(Configuration conf){
			String encoding = 
					conf.get(MGIFastqInputFormat.CONF_BASE_QUALITY_ENCODING,
							conf.get(FormatConstants.CONF_INPUT_BASE_QUALITY_ENCODING,
								      MGIFastqInputFormat.CONF_BASE_QUALITY_ENCODING_DEFAULT));
			if ("illumina".equals(encoding))
				qualityEncoding = BaseQualityEncoding.Illumina;
			else if ("sanger".equals(encoding))
				qualityEncoding = BaseQualityEncoding.Sanger;
			else
				throw new RuntimeException("Unknown input base quality encoding value " + encoding);

		}
		/*
		 * Position the input stream at the start of the first record.
		 */
		private void positionAtFirstRecord(FSDataInputStream stream) throws IOException
		{
			if (start >0)
			{
				// Advance to the start of the first record
				// We use a temporary LineReader to read lines until we find the
				// position of the right one. We then seek the file to that position.
				stream.seek(start);
				LineReader reader = new LineReader(stream);
				
				int bytesRead  = 0;
				do {
					bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end-start));
					if (bytesRead > 0 && (buffer.getLength() <= 0 || buffer.getBytes()[0] != '@'))
						start += bytesRead;
					else
					{
						// line starts with @.  Read two more and verify that it starts with a +
						//
						// If this isn't the start of a record, we want to backtrack to its end
						long backtrackPosition = start + bytesRead;

						bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));
						bytesRead = reader.readLine(buffer, (int)Math.min(MAX_LINE_LENGTH, end - start));
						if (bytesRead > 0 && buffer.getLength() > 0 && buffer.getBytes()[0] == '+')
							break; // all good!
						else
						{
							// backtrack to the end of the record we thought was the start.
							start = backtrackPosition;
							stream.seek(start);
							reader = new LineReader(stream);
						}
					}
				}while (bytesRead > 0);
				
				stream.seek(start);
			}
			// else
			// if start == 0 we presume it starts with a valid fastq record
			pos = start;
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
			inputStream.close();
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
	
	@Override
	public boolean isSplitable(JobContext context, Path path)
	{
		CompressionCodec codec = new CompressionCodecFactory(context.getConfiguration()).getCodec(path);
		return codec == null;
	}
	
	@Override
	public RecordReader<Text, MGISequencedFragment> createRecordReader(InputSplit genericSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		context.setStatus(genericSplit.toString());
		return new FastqRecordReader(context.getConfiguration(), (FileSplit)genericSplit);
	}
	

}
