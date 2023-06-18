# Assigment

# How to start hdfs:

```cmd
start-dfs.sh
start-yarn.sh
```

# How to check if services are running:

```cmd
jps
```

# HDFS Url:

[http://localhost:9870/](http://localhost:9870/)

# Yarn URL:

[http://localhost:8088/](http://localhost:8088/)

# How to put data into hdfs:

```cmd
hadoop fs -put -f <src-file> <dest-dir>
```

# How to check JAR:

```cmd

jar -tvf Build.jar

```

# How to run:

```cmd
hadoop jar Build.jar /mapper-assignment-2/Phone.txt /mapper-assignment-2/output
```

# How to Read:

```cmd
hadoop fs -cat <hdfs_file_path>
```

# How to download file:

```cmd
hadoop fs -get <hdfs_file_path> <local_destination_path>
```