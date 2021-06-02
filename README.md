# StereomicsSpark
mapping barcode to stereo_chip

##run
```
usage: spark-submit --master yarn --executor-memory 64G --executor-cores 2 --class engine.main StereomicsSpark_1.0.jar \
        -m maskFile -r1 read1.fq -r2 read2.fq -o result --stat result.stat --outPartition 100  
 -adapterFile <arg>    adapter file in fasta format, when this option was
                       given, program will discard reads with adapter
                       sequence in this adapterFile.
 -barcodeLen <arg>     barcode length.
 -barcodeRead <arg>    1 means barcode sequence locates in the read1 and 2
                       means barcode sequence locates in the read2.
 -barcodeStart <arg>   barcode start position in the read sequence,
                       0-based indexing.
 -h,--help             list short help
 -m,--mask <arg>       stereomics chip mask file.
 -o,--out <arg>        output file path.
 -outPartition <arg>   partition number of the output file. If not
                       specified, remain the original partition before
                       writing
 -r1,--read1 <arg>     fastq file of the second sequencing read1.
 -r2,--read2 <arg>     fastq file of the second sequencing read2.
 -stat <arg>           statistic file path.
 -umiLen <arg>         umi length.
 -umiRead <arg>        1 means umi sequence locates in the read1 and 2
                       means umi sequence locates in the read2
 -umiStart <arg>       umi start position in the read sequence, 0-based
                       indexing.
```

