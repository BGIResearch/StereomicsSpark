package utils;

import java.io.Serializable;

import org.apache.spark.util.LongAccumulator;

public class Utils implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public static final char BaseArray[] = {'A', 'C', 'T', 'G'};
	public static final char Q10 = '+';
	public static final char Q20 = '5';
	public static final char Q30 = '?';

	public Utils() {
		// TODO Auto-generated constructor stub
	}
	
	/*
	keep this 2 bits
					  ||
	A		65	01000|00|1	0
	C		67	01000|01|1	1
	G		71	01000|11|1	3
	T		84	01010|10|0	2
	*/
	
	public static long encode(String sequence, int start, int length) {
		long value = 0;
		long tmp = 0;
		for (int i = start; i < start+length; i++) {
			tmp = (sequence.charAt(i) & 6) >> 1; //6: ob00000110	
			value |= (tmp << (i*2));
		}
		return value;
	}
	
	public static long encode(String sequence) {
		return encode(sequence, 0, sequence.length());
	}
	
	public static String decode(long seqInt, int seqLen) {
		int tmp;
		StringBuilder seqs = new StringBuilder();
		for (int i = 0; i < seqLen; i++) {
			tmp = (int) ((seqInt >> (i*2)) & 3); //3 ob00000011
			seqs.append(BaseArray[tmp]);
		}
		return seqs.toString();
	}
	
	public static boolean umiQCpass(String umiSeq, String umiQual) {
		int q10BaseCount = 0;
		int baseAcount = 0;
		for (int i = 0; i<umiSeq.length(); i++) {
			if (umiSeq.charAt(i) == 'N' || umiSeq.charAt(i) == 'n') {
				return false;
			}else if (umiSeq.charAt(i) == 'A' || umiSeq.charAt(i) == 'a') {
				baseAcount++;
			}
			if (umiQual.charAt(i) < Q10) {
				q10BaseCount++;
				if (q10BaseCount>1) {
					return false;
				}
			}
		}
		if (baseAcount == umiSeq.length()) {
			return false;
		}
		return true;
	}
	
	public static int seqQualityStat(String seqQual, int start, int length) {
		int q30BaseCount = 0;
		for (int i = start; i< start+length; i++) {
			if (seqQual.charAt(i) >= Q30) {
				q30BaseCount++;
			}
		}
		return q30BaseCount;
	}

}
