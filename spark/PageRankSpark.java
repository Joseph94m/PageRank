package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import com.google.common.collect.Iterables;
import scala.Tuple2;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import java.util.Date;

public class PageRankSpark {
	public static void main(String[] args) throws ParseException {
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'", Locale.ENGLISH);

		String sd = args[3];
		final Date userDate = df.parse(sd);
		SparkConf conf = new SparkConf().setAppName("PageRank");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		Configuration hadoopConf = new Configuration();
		hadoopConf.set("textinputformat.record.delimiter", "\n\n");
		JavaRDD<Text> rdd = jsc
				.newAPIHadoopFile(args[0], TextInputFormat.class, LongWritable.class, Text.class, hadoopConf).values();
		// Union guarantees that pages with only one revision are not dropped during the
		// reduceByKey routine
		// Other (better) ways could have been used to solve this problem, but after
		// running the code on the large sample
		// it didn't take much time so we didn't change it.
		JavaRDD<Text> rdd_union = rdd.union(rdd);
		JavaPairRDD<String, Iterable<String>> links;
		// links contains the list of outlinks and their target inlinks
		links = rdd_union.mapToPair(s -> {
			String[] lines = s.toString().split("\n");
			String[] revLine = lines[0].split(" ");
			return new Tuple2<String, String>(revLine[3],
					revLine[4] + "~~" + ((lines[3]).length() > 5 ? (lines[3]).substring(5) : ""));
		})

				.reduceByKey((time_outlinks_1, time_outlinks_2) -> {// the following code block keeps the revision with
																	// the latest time IF applicable otherwise returns
																	// "none"
					Date date_1 = null;
					Date date_2 = null;
					if (!time_outlinks_1.equals("none")) {
						date_1 = df.parse(time_outlinks_1.split("~~")[0]);
					}
					if (!time_outlinks_2.equals("none")) {
						date_2 = df.parse(time_outlinks_2.split("~~")[0]);
					}
					if (date_1 != null) {
						if (date_1.before(userDate)) {
							if (date_2 != null) {
								if (date_2.before(userDate)) {
									if (date_1.after(date_2)) {
										return time_outlinks_1;
									} else {
										return time_outlinks_2;
									}
								} else {
									return time_outlinks_1;
								}
							}
							return time_outlinks_1;
						}
					}
					if (date_2 != null) {
						if (date_2.before(userDate)) {
							return time_outlinks_2;
						}
					}
					return "none";
				}).cache()
				.mapToPair(s -> new Tuple2<String, String>(s._2.split("~~").length > 1 ? s._1 : "",
						s._2.split("~~").length > 1 ? s._2.split("~~")[1] : "")) // returns a pair value :<Article
																					// name,list of target article
																					// names> or <"",""> if there are no
																					// outlinks for that article
				.flatMapToPair(s -> {
					List<Tuple2<String, String>> res = new ArrayList<Tuple2<String, String>>();
					String[] outlinks = s._2.split(" ");
					for (String str : outlinks) {
						if (!outlinks[0].equals(str) && !outlinks[0].equals("")) { // removes self loops and empty stuff
							res.add(new Tuple2<String, String>(outlinks[0], str)); // returns a pair <article name,
																					// target article name>
						} 
					}
					return res;
				}).distinct() // removes multiple links from the same article. also serves to remove empty
								// tuples
				.groupByKey().cache();
		
		//The following few lines guarantee that pages with NO OUTLINKs  still receive a score
		JavaPairRDD<String, Double> ranks_orig = links.mapValues(s -> {
			return 1.0;
		});

		JavaPairRDD<String, String> ranks_tmp = links
				.mapValues(s -> s.toString().substring(1, s.toString().length() - 1)); // substring is to remove the brackets from the article name e.g. [link]
		JavaPairRDD<String, String> ranks_outs = ranks_tmp.mapToPair(f -> f.swap());
		JavaPairRDD<String, Double> ranks = ranks_orig.union(ranks_outs.mapValues(s -> {
			return 1.0;
		}));
		ranks=ranks.distinct();

		// The loop computes the pagerank of each page (taken from the lecture notes)
		int numIterations = Integer.parseInt(args[2]);
		for (int current = 0; current < numIterations; current++) {
			JavaPairRDD<String, Double> contribs = links.join(ranks).values().flatMapToPair(v -> {
				List<Tuple2<String, Double>> res = new ArrayList<Tuple2<String, Double>>();
				int urlCount = Iterables.size(v._1);
				for (String s : v._1)
					if (!s.equals("")) {
						res.add(new Tuple2<String, Double>(s, v._2() / urlCount));
					}
				return res;
			});
			ranks = contribs.reduceByKey((a, b) -> a + b).mapValues(v -> 0.15 + v * 0.85);
		}

		// We would like to see the results ordered by their pagerank but there is no
		// sort by value function in java (only in scala) so we had to swap the keys and
		// the values
		// and then order and then swap.
		ranks.mapToPair(f -> f.swap()).sortByKey(false).mapToPair(f -> f.swap()).map(f -> f._1 + " " + f._2)
				.saveAsTextFile(args[1]);
		jsc.close();
	}
}
