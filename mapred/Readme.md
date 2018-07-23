Joseph Moukarzel
Constantinos Ioannidis

This folder contains 3 classes in 1 package named mapreduce.  
	1)MultiLineInputFormat  
	2)PageRank  
	3)PageRankComputer  
	
----------------------------------Details for each class----------------------------------  

	1)MultiLineInputFormat: This class extends NLineInputFormat and contains a static class that extends RecordReader. It is needed because each record contains 14 lines and we need to make sure that these 14 lines are sent to the same mapper in PageRank class. We adapted code that we found on https://stackoverflow.com/questions/2711118/multiple-lines-of-text-to-a-single-map  
  
        2) PageRank: Reads the input path which is divided by records of 14 line each.
a)Mapper of PageRank: Goes through all the lines it receives and logically splits them into records of 14 lines using MultiLineInputFormat. It emits the follow <key,value> : <article_name of each record, revision_id+ " " + the outlinks for that article for that revision)
b)Reducer of PageRank: Searches for the article_name with the highest revision_ id and emits the following <key,value> : <article_name, "1" + the outlinks of this article for the newest revision>  
  

	3)PageRankComputer: Reads the output of PageRank (for the first iteration). Each record is now one line and contains the article name, the score (initially set to 1 from PageRank) and the outlinks.
a)Mapper for PageRankComputer: Emits two kinds of <key,value> pairs:
				i)For each outlink, emit <outlink name, score of (mother) article (1 if first read) +" "+ number of outlinks of (mother) article>  
				ii) Emit < (mother) article_name,list of outlinks>  
b)Reducer for PageRankComputer:For each article name it reads input that has the following shape:  
			        i) <article_name, score of some article that points to this article+" "+  number of outlinks for the article that points to this article>  
			        ii) <article_name, outlinks for this article>  
The reducer then computes the PageRank using the function: P(article)= 0.15+ 0.85*sum(P(an article that points toward this article)/number of outlinks for the article that points to this article)  
The reducer finally emits an output similar to that the PageRankComputer mapper received, so: <article_name, score+" "+ outlinks>. Note that  for the last iteration only <article_name, score> is written. This is done by creating a boolean and passing it to the configuration job2.getConfiguration().setBoolean("is.last.job", false); and setting it to true on the last run.  

  
----------------------------------Example of use----------------------------------  
1) set the HADOOP_CLASSPATH to target the jar  
2) Run hadoop mapreduce.PageRank /path/to/input.txt /path/to/output #number of iterations  
  


----------------------------------Things that one should be aware of----------------------------------  
1)The output of PageRank job will be written to a folder called results_0  
2)PageRankComputer will read results_0 and will write to files named results_i where i>0, the final iteration PageRankComputer will write to /path/to/output specified.  
3)Since we are using MultiLineInputFormat for PageRank, the user might want to specify the number of lines that each mapper receives. This is coded in the run function of PageRank like that :NLineInputFormat.setNumLinesPerSplit(job, 163100); Note that this number MUST be a multiple of 14 because every PageRank mapper must receive all the lines of a single record. In our case, each mapper is receiving 163100/14 = 11650 records. This experiment was done using the 1% data of wikipedia. It results in around 100 mappers. When running the mapper on the smallest data ~ 0.05%, you could use 6300 as the input for MultiLinesPerSplit, it will results in ~ 87 mappers. The number of mappers for PageRankComputer is left to its default settings.  
  
----------------------------------Experiment conducted----------------------------------  
We ran our experiment on the 1% sample data with 15 iterations. it took around 20 minutes to complete. We know that this number can be further reduced by optimizing some parts of our code where we parse Strings, and the speed might also be boosted by tweaking the number of mappers and reducers.  
  
The pages with the highest score were:  
United_States	681.7165110596891   
2006	460.7925879548616   
2007	417.53378213945876  
2005	353.0109946287608   
United_Kingdom	291.10379991266666   
2004	267.1745423901594  
England	244.18825150116885  
France	225.54432723779232   
2003	220.57487342705963   
Canada	218.16411125793903   
Germany	217.8583319608347  
2000	206.10417221546723  
Australia	185.65178807429453  
2001	175.75649679604982  
2002	175.44272529954844  
Japan	173.12458457248366   
India	164.43481013574618   
World_War_II	158.47965991758156   
 



 
 




