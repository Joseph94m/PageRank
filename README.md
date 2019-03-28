# PageRank

This repository provides 2 PageRank implementations for the internal wikipedia graph. One in Spark and one in Hadoop MapReduce


### This repository is for indicative purposes only! Do not use it if you plan on cheating!
## InputFormat


• REVISION: revision metadata, consisting of:

&nbsp; &nbsp; &nbsp; &nbsp; o article_id: a large integer, uniquely identifying each page.  

&nbsp; &nbsp; &nbsp; &nbsp; o rev_id: a large number uniquely identifying each revision.  

&nbsp; &nbsp; &nbsp; &nbsp; o article_title: a string denoting the page’s title (and the last part of the URL of the page).

&nbsp; &nbsp; &nbsp; &nbsp; o timestamp: the exact date and time of the revision, in ISO 8601 format; e.g., 13:45:00
UTC 30 September 2013 becomes 2013-09-12T13:45:00Z, where T separates the date from the time part and Z denotes the time is in UTC.  

&nbsp; &nbsp; &nbsp; &nbsp; o [ip:]username: the name of the user who performed the revision, or her DNS-resolved IP address (e.g., ip:office.dcs.gla.ac.uk) if anonymous.  

&nbsp; &nbsp; &nbsp; &nbsp; o user_id: a large number uniquely identifying the user who performed the revision, or her IP address as above if anonymous.      

• CATEGORY: list of categories this page is assigned to.  

• IMAGE: list of images in the page, each listed as many times as it occurs.  

• MAIN, TALK, USER, USER_TALK, OTHER: cross-references to pages in other namespaces.  

• EXTERNAL: list of hyperlinks to pages outside Wikipedia.  

• TEMPLATE: list of all templates used by the page, each listed as many times as it occurs.  

• COMMENT: revision comments as entered by the revision author.  

• MINOR: a Boolean flag (0|1) denoting whether the edit was marked as minor by the author.  

• TEXTDATA: word count of revision's plain text.  

• An empty line, denoting the end of the current record.     



## Formula
PR(u)=0.15 + 0.85 * Sum(PR(v)/L(v)), ∀v: ∃(v,u) ∈S, where L(v) is the number of out-links of page v.

## OutputFormat
 Article_1 score1
 
 Article_2 score2
 
 Article_3 score3
