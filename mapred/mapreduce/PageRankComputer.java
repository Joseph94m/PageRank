package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;

public class PageRankComputer extends Configured implements Tool {
	
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}
			
		/*
		 * 
		 * Parameters: key: identifies the input (not used)
		 * value : Contains lines with the following format : <article_name, score and list of outlinks> e.g. <United_kingdom, 30 United_States France England>
		 * 
		 * Output: <article_name,~ outlinks> e.g. <United_Kingdom, ~United_States France England>
		 *         <outlink_name, rank and number of outlinks for the current article_name> e.g. <United_States, 30 3> 
		 * 
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String s = value.toString();
			StringTokenizer tk = new StringTokenizer(s, "\n");
				while (tk.hasMoreTokens()) {
					String line_1 = tk.nextToken();
					StringTokenizer line_1_content = new StringTokenizer(line_1);
					StringTokenizer to_send = new StringTokenizer(line_1);
					String article_name = line_1_content.nextToken();
						String rank = line_1_content.nextToken();
						int number = line_1_content.countTokens();
						to_send.nextToken();
						to_send.nextToken();
						String aggregator = "";
						while (to_send.hasMoreTokens()) {
							aggregator += to_send.nextToken() + " ";
						}

						while (line_1_content.hasMoreTokens()) {
							context.write(new Text(line_1_content.nextToken()), new Text(rank + " " + number));
						}
						context.write(new Text(article_name), new Text("~ " + aggregator));
				}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	static class MyReducer extends Reducer<Text, Text, Text, Text> {
		
		private boolean isLastJob =false;

		@Override
		
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			Configuration conf = context.getConfiguration();
			isLastJob = conf.getBoolean("is.last.job",false);
		}

		/*
		 * 
		 * Parameters: key: identifies the input (not used)
		 * value : contains some number of lines, Each line is a record
		 * value has the following form: <article_name,~ outlinks> e.g. <United_Kingdom, ~United_States France England>
		 *                               <outlink_name, rank and number of outlinks of the poiting article> e.g. <United_Kingdom, 25 6>, <United_Kingdom, 47 2>,etc..
		 * 
		 * Output: <article_name,score of this article and outlinks> e.g. <United_kingdom, 27.66=(25/6+47/2) United_States France England >
		 *         
		 * 
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double b = 0.15;
			double k = 0;
			String links = "";
			boolean passed = false;
			for (Text v : values) {
				StringTokenizer st = new StringTokenizer(v.toString());
				String tmp = st.nextToken();
				if (tmp.equals("~") ) {
					if (!passed && !isLastJob ) {
						passed = true;
						while (st.hasMoreTokens()) {
							links += st.nextToken() + " ";
						}
					}
				} else {
					k += Double.parseDouble(tmp) / Double.parseDouble(st.nextToken());
				}
			}
			double score = 0.15 + 0.85 * k;
			context.write(key, new Text(score + " " + links));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	public int run(String[] args) throws Exception {
		return 0;
	}

	public static void main(String[] args) throws Exception {
	}
}
