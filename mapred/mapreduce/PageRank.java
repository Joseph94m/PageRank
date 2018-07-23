package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PageRank extends Configured implements Tool {

	static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
		}
		/*
		 * 
		 * Parameters: key: identifies the input (not used)
		 * value : Contains linespermap number of lines, so linespermap/14 records. Each record is treated separately.
		 * 
		 * Output: <article_name, revision id and list of outlinks for this record>
		 *        
		 * 
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String s = value.toString();
			StringTokenizer tk = new StringTokenizer(s, "\n");
			int linespermap = context.getConfiguration().getInt("mapreduce.input.lineinputformat.linespermap", -1);
			for (int ak = 0; ak < linespermap/14; ++ak) {
				if (tk.hasMoreTokens()) {
					String line_1 = tk.nextToken();
					StringTokenizer line_1_content = new StringTokenizer(line_1);
					line_1_content.nextToken();
					line_1_content.nextToken();
					String revision_id = line_1_content.nextToken();
					String article_name = line_1_content.nextToken();
					tk.nextToken();
					tk.nextToken();
					String line_4 = tk.nextToken();
					StringTokenizer st = new StringTokenizer(line_4);
					st.nextToken();
					String res = "";
					while (st.hasMoreTokens()) {
						res += st.nextToken() + " ";
					}
					context.write(new Text(article_name), new Text(revision_id + " " + res));
					for (int t = 0; t < 9; ++t) {
						if (tk.hasMoreTokens()) {
							tk.nextToken();
						} else
							break;
					}
				} else
					break;
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}

	static class MyReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			super.setup(context);
			// ...
		}
		/*
		 * 
		 * Parameters: key: identifies the input (not used)
		 * value : contains some number of lines, Each line is a record of the form <article name, revision id and list of outlinks>
		 * 
		 * Output: <article_name,1 and outlinks>
		 *         
		 * 
		 */
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			long a = -1;
			String b = "";
			for (Text v : values) {
				String s = v.toString();
				StringTokenizer st = new StringTokenizer(s);
				String rev = st.nextToken();
				long rev_id = Long.parseLong(rev);
				if (a < rev_id) {
					a = rev_id;
					b = v.toString().substring(rev.length());
				}
			}
			context.write(key, new Text("1 " + b));
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// ...
			super.cleanup(context);
		}
	}
	
	/*
	 * 
	 * First run the PageRank job and write to results_0
	 * The user can change the number of LinesPerSplit
	 * 
	 * Second, PageRankComputer jobs are chained, intermediate results are written to
	 * results_1...results_2...etc.. The final results is written to the output specified.
	 */
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf(), "PageRank");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(MultiLineInputFormat.class);
		// the input number for the following call MUST BE A MULTIPLE OF 14
		NLineInputFormat.setNumLinesPerSplit(job, 163100);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path("results_0"));
		job.setNumReduceTasks(80);

		boolean succeeded = false;
		succeeded = job.waitForCompletion(true);
		int numLoops = Integer.parseInt(args[2]);
		String final_output = "";
		int j = 1;
		for (int i = 0; i < numLoops; ++i) {
			Job job2 = Job.getInstance(getConf(), "PageRankComputer");
			job2.getConfiguration().setBoolean("is.last.job", false);
			if (i == 0) {
				final_output = args[1];
				args[1] = "results_1";
				args[0] = "results_0";
			}
			if (i == numLoops - 1) {
				args[1] = final_output;
				job2.getConfiguration().setBoolean("is.last.job", true);
			}

			
			job2.setJarByClass(PageRankComputer.class);
			job2.setMapperClass(PageRankComputer.MyMapper.class);
			job2.setReducerClass(PageRankComputer.MyReducer.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			FileInputFormat.setInputDirRecursive(job2, true);
			FileInputFormat.addInputPath(job2, new Path(args[0]));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			job2.setNumReduceTasks(80);
			succeeded = job2.waitForCompletion(true);
			args[0] = args[1];
			++j;
			args[1] = "results_" + j;
		}
		return (succeeded ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new PageRank(), args);
	}
}