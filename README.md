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
ssh -p 22 -l <USER_ID> -N -L 19888:c100.local:19888 -L 8088:c100.local:8088 lbd.zserv.tuwien.ac.at
```

add to `/etc/hosts`:
```
127.0.0.1     c100.local
```

browse to http://c100.local:8088/cluster/apps and after selecting a job for running job goto **ApplicationMaster**, for finished job goto to **history** and then to **logs**
