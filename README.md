# Some Hadoop Commands
hdfs:///user/elmar/amazon-reviews/full/complete/reviewscombined.json


hadoop jar mr-job.jar MrJob /user/elmar/amazon-reviews/full/complete/reviewscombined.json /user/dic/2019S/users/e00125536g/mrjob
hadoop jar mr-job.jar /user/elmar/amazon-reviews/full/complete/reviewscombined.json /user/dic/2019S/users/e00125536g/mrjob2

hadoop fs -mkdir hdfs:///user/dic/2019S/users/e00125536g/mrjob
hadoop fs -ls hdfs:///user/dic/2019S/users/e00125536g/mrjob6




hadoop jar catcount.jar /user/elmar/amazon-reviews/full/complete/reviewscombined.json /user/dic/2019S/users/e00125536g/catcount2
hadoop fs -getmerge /user/dic/2019S/users/e00125536g/catcount2 ~/categories.txt



# port forwarding

```
ssh -p 22 -l $USER -N -L 19888:c100.local:19888 -L 8088:c100.local:8088 lbd.zserv.tuwien.ac.at
```

add to `/etc/hosts`:
```
127.0.0.1     c100.local
```

browse to http://c100.local:8088/cluster/apps and after selecting a job for running job goto **ApplicationMaster**, for finished job goto to **history** and then to **logs**

```
hadoop jar dr-who-1.0-SNAPSHOT-jar-with-dependencies.jar /user/elmar/amazon-reviews/full/complete/reviewscombined.json /user/dic/2019S/users/$USER/drwho-4

yarn logs -applicationId application_1553174680985_2025 | grep DrWho
```

```
hadoop fs -cat /user/elmar/amazon-reviews/full/complete/reviewscombined.json | head -n 20 > reviews-tiny.json

hadoop fs -put reviews-tiny.json hdfs:///user/dic/2019S/users/$USER
```

```
hadoop jar dr-who-1.0-SNAPSHOT-jar-with-dependencies.jar /user/dic/2019S/users/$USER/reviews-tiny.json /user/dic/2019S/users/$USER/drwho-6

yarn logs -applicationId application_1553174680985_2025 | grep DrWho
```