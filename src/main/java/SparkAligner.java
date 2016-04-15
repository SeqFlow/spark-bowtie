import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.seqdoop.hadoop_bam.FastqInputFormat;
import org.seqdoop.hadoop_bam.SequencedFragment;

/**
 * Created by pfxuan on 4/14/16.
 */
public class SparkAligner {

  public static void main(String[] args) {

    // Step 1: Create a Java Spark Context
    SparkConf conf = new SparkConf().setAppName("Spark-Bowtie Example").setMaster("local");
    JavaSparkContext sc = new JavaSparkContext(conf);

    // Step 2: Load fastq file using Hadoop-BAM
    JavaPairRDD<Text, SequencedFragment> reads =
        sc.newAPIHadoopFile("src/test/resources/reads.fq", FastqInputFormat.class, Text.class, SequencedFragment.class, new Configuration());

    // Step 3: Reconstruct reads (readsPipe is connected to bowtie stdin)
    JavaRDD<String> readsPipe = reads.map(r ->
        "@" + r._1.toString() + System.getProperty("line.separator")
        + ((SequencedFragment) r._2).getSequence().toString() + System.getProperty("line.separator")
        + "+" + System.getProperty("line.separator")
        + ((SequencedFragment) r._2).getQuality()
    );

    // Step 4: Call external application (alignmentPipe is connected to bowtie stdout)
    JavaRDD<String> alignmentPipe = readsPipe.pipe("bin/bowtie -S src/test/resources/NC_008253 -");

    // Step 5: Downstream processing on SAM output
    alignmentPipe.foreach(e -> System.out.println(e));
  }
}
