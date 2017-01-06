# HBase-real-time-streaming-utility-with-Oozie-scheduler

This utility will help you to monitor and fetch data from Hbase and processing it for real time analysis. This will divides continuously flowing input data into discrete units for processing. It helps to reduce the batch processing delay and gives us the ability to serve the data for analysis / dashboards / grids.

This utility dose the following.

1. Read HBase rows based on configured time range and interval.
2. Keep track of statistics information such as how many records proceeded , last read data time stamp etc.
3. Process data.
4. Supply processed data to real world data applications.

In this system , work flow is configured using Oozie and the time -based coordinator job is  defined to run on regular time intervals .
