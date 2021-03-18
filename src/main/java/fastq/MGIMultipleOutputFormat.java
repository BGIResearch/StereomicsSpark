package fastq;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

import scala.Tuple2;
import structures.Position;

public class MGIMultipleOutputFormat extends MultipleTextOutputFormat<Position, Tuple2<Text, MGISequencedFragment>>{
	
	protected int partitionNum;
	
}
