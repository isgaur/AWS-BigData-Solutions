Suppose you have an EMR cluster and you submit a Spark application to it from your local laptop. You will have:

* The logs of the Spark client, as in the logs you will see in your terminal in your local laptop while the application is running.
* The logs of the Spark driver, both for the STDOUT and STDERR streams
* The logs of each one of the Spark executors, both for the STDOUT and STDERR streams

If you enable continuous logging:

* While the job is running, you will be able to see the progress bar in the web console
* Output will take you to the log stream of the Spark client
* Logs will take you to a log group where you will find all the STDOUT streams of the driver and each of the execuotrs
* Error logs will take you to a log group where you will find all the STDERR streams of the driver and each of the executors

If you don't:

* Output will not be present
* Logs will take you to a log stream containing the combination of the client logs and all the STDOUT streams of the driver and each of the executors
* Error logs will take you to a log stream containing the combination of the STDERR streams of the driver and each of the executors
