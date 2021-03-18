package fastq;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.seqdoop.hadoop_bam.FormatConstants;
import org.seqdoop.hadoop_bam.FormatException;
import org.seqdoop.hadoop_bam.FormatConstants.BaseQualityEncoding;

public class MGISequencedFragment implements Writable{

    protected Text sequence = new Text();
    protected Text quality = new Text();
    protected String umi;
    protected String umiQual;
    protected String coordinate;
    protected Integer read;

    public void clear() {
        sequence.clear();
        quality.clear();
        umi = null;
        umiQual = null;
        coordinate = null;
        read = null;
    }

    public String getCoordinate() {
        return coordinate;
    }

    public void setCoordinate(String coordinate) {
        this.coordinate = coordinate;
    }

    /**
     * Get sequence Text object.
     * Trade encapsulation for efficiency.  Here we expose the internal Text
     * object so that data may be read and written diretly from/to it.
     *
     * Sequence should always be written using CAPITAL letters and 'N' for unknown bases.
     */

    public Text getSequence() {return sequence;}

    /**
     * Get quality Text object.
     * Trade encapsulation for efficiency.  Here we expose the internal Text
     * object so that data may be read and written diretly from/to it.
     *
     * Quality should always be in ASCII-encoded Phred+33 format (sanger).
     */

    public Text getQuality() {return quality;}
    public Integer getRead() {return read;}

    public void setSequence(Text seq) {
        if (seq == null) {
            throw new IllegalArgumentException("can't have a null sequence");
        }
        sequence = seq;
    }

    /**
     * Set quality.  Quality should be encoded in Sanger Phred+33 format.
     */
    public void setQuality(Text qual) {
        if (qual == null) {
            throw new IllegalArgumentException("can't have a null quality");
        }
        quality = qual;
    }

    public void setRead(Integer v) { read = v;}



    public String getUmi() {
        return umi;
    }

    public void setUmi(String umi) {
        this.umi = umi;
    }

    public String getUmiQual() {
        return umiQual;
    }

    public void setUmiQual(String umiQual) {
        this.umiQual = umiQual;
    }

    /**
     * Recreates a pseudo qseq record with the fields available.
     */
    public String toString() {
        String delim = "\t";
        StringBuilder builder = new StringBuilder(800);
        builder.append(read).append(delim);
        builder.append(sequence).append(delim);
        builder.append(quality);
        return builder.toString();
    }

    public boolean equals(Object other) {
        if (other != null && other instanceof MGISequencedFragment) {
            MGISequencedFragment otherFrag = (MGISequencedFragment) other;
            if (read == null && otherFrag.read != null || read != null && !read.equals(otherFrag.read))
                return false;
            if(!sequence.equals(otherFrag.sequence))
                return false;
            if (!quality.equals(otherFrag.quality))
                return false;
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = sequence.hashCode();
        result = 31*result + quality.hashCode();
        result = 31*result + (read != null ? read.hashCode() : 0);
        return result;
    }

    /**
     * Convert quality scores in-place.
     *
     * @throws FormatException if quality scores are out of the range
     * allowed by the current encoding.
     * @throws IllegalArgumentException if current and  target quality encodings are the same.
     */

    public static void convertQuality(Text quality, BaseQualityEncoding current, BaseQualityEncoding target)
    {
        if (current == target)
            throw new IllegalArgumentException("current and target quality encodinds are the same (" + current + ")");

        byte[] bytes = quality.getBytes();
        final int len = quality.getLength();
        final int illuminaSangerDistance = FormatConstants.ILLUMINA_OFFSET - FormatConstants.SANGER_OFFSET;

        if (current == BaseQualityEncoding.Illumina && target == BaseQualityEncoding.Sanger)
        {
            for (int i = 0; i < len; ++i)
            {
                if (bytes[i] < FormatConstants.ILLUMINA_OFFSET || bytes[i] > (FormatConstants.ILLUMINA_OFFSET + FormatConstants.ILLUMINA_MAX))
                {
                    throw new FormatException(
                            "base quality score out of range for Illumina Phred+64 format (found " + (bytes[i] - FormatConstants.ILLUMINA_OFFSET) +
                                    " but acceptable range is [0," + FormatConstants.ILLUMINA_MAX + "]).\n" +
                                    "Maybe qualities are encoded in Sanger format?\n");
                }
                bytes[i] -= illuminaSangerDistance;
            }
        }
        else if (current == BaseQualityEncoding.Sanger && target == BaseQualityEncoding.Illumina)
        {
            for (int i = 0; i < len; ++i)
            {
                if (bytes[i] < FormatConstants.SANGER_OFFSET || bytes[i] > (FormatConstants.SANGER_OFFSET + FormatConstants.SANGER_MAX))
                {
                    throw new FormatException(
                            "base quality score out of range for Sanger Phred+64 format (found " + (bytes[i] - FormatConstants.SANGER_OFFSET) +
                                    " but acceptable range is [0," + FormatConstants.SANGER_MAX + "]).\n" +
                                    "Maybe qualities are encoded in Illumina format?\n");
                }
                bytes[i] += illuminaSangerDistance;
            }
        }
        else
            throw new IllegalArgumentException("unsupported BaseQualityEncoding transformation from " + current + " to " + target);
    }

    /**
     * Verify that the given quality bytes are within the range allowed for the specified encoding.
     *
     * In theory, the Sanger encoding uses the entire
     * range of characters from ASCII 33 to 126, giving a value range of [0,93].  However, values over 60 are
     * unlikely in practice, and are more likely to be caused by mistaking a file that uses Illumina encoding
     * for Sanger.  So, we'll enforce the same range supported by Illumina encoding ([0,62]) for Sanger.
     *
     * @return -1 if quality is ok.
     * @return If an out-of-range value is found the index of the value is returned.
     */

    public static int verifyQuality(Text quality, BaseQualityEncoding encoding)
    {
        // set allowed quality range
        int max, min;

        if (encoding == BaseQualityEncoding.Illumina)
        {
            max = FormatConstants.ILLUMINA_OFFSET + FormatConstants.ILLUMINA_MAX;
            min = FormatConstants.ILLUMINA_OFFSET;
        }
        else if (encoding == BaseQualityEncoding.Sanger)
        {
            max = FormatConstants.SANGER_OFFSET + FormatConstants.SANGER_MAX;
            min = FormatConstants.SANGER_OFFSET;
        }
        else
            throw new IllegalArgumentException("Unsupported base encoding quality " + encoding);

        // verify
        final byte[] bytes = quality.getBytes();
        final int len = quality.getLength();

        for (int i = 0; i < len; ++i)
        {
            if (bytes[i] < min || bytes[i] > max)
                return i;
        }
        return -1;
    }

    public void write(DataOutput out) throws IOException {
        // TODO reimplement with a serialization system (e.g. Avro)
        sequence.write(out);
        quality.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        // TODO reimplement with a serialization system (e.g. Avro)

        // serialization order;
        // 1)sequence
        // 2)quality

        this.clear();
        sequence.readFields(in);
        quality.readFields(in);
    }

}