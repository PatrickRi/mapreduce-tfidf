# Some Hadoop Commands
hdfs:///user/elmar/amazon-reviews/full/complete/reviewscombined.json


hadoop jar mr-job.jar MrJob /user/elmar/amazon-reviews/full/complete/reviewscombined.json /user/dic/2019S/users/e00125536g/mrjob
hadoop jar mr-job.jar /user/elmar/amazon-reviews/full/complete/reviewscombined.json /user/dic/2019S/users/e00125536g/mrjob2

hadoop fs -mkdir hdfs:///user/dic/2019S/users/e00125536g/mrjob
hadoop fs -ls hdfs:///user/dic/2019S/users/e00125536g/mrjob6




hadoop jar catcount.jar /user/elmar/amazon-reviews/full/complete/reviewscombined.json /user/dic/2019S/users/e00125536g/catcount2
hadoop fs -getmerge /user/dic/2019S/users/e00125536g/catcount2 ~/categories.txt
