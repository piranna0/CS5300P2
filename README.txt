Ansha Yu ay226
Gene Shin gws55
Jimmy Feher jkf49

Running details:
BlockedPagerank has a number of fields. These can be input using 4 arguments in this order
String bucket; //S3 bucket
String inputKey; //S3 path
String outputDirectory; //Filepath, not S3
String finalOutputKey; //S3 path

In the method writeS3, you must put your own public and secret key. We had trouble using the AwsCredentials.properties file on 
aws mapreduce.

PageRank using AWS Elastic MapReduce

Simple PageRank
- Mappers and reducers both have the following input/output types 
  - Key is node number of type Text
  - Value is a string (type Text) containing the node's PageRank value and outgoing nodes, all delimited by "_“.
- Map input: 
  <u;PR(u)_{v | u->v}>
- Map outputs/ Reducer input: 
  <u;PR(u)_{v | u->v}>, <{v;PR(u)/N_-1} | u->v>
- Reducer output: 
  <v;Prnew(v)_{w | v->w}>
- To pass values between mappers and reducers, we construct a value string (the node’s PageRank and outgoing nodes) when emitting.
- When a mapper or reducer receives it, it can parse them out of the string.
- Hadoop Counters approach to accumulate the average residuals.

Simple PageRank Functionality
- Single nodes are passed through MapReduce phases and PageRanks are calculated individually, node by node.
- To gauge how quickly (rather, how slowly) simple PageRank converges, we calculate the average of the residuals for each node for each phase. The residual is defined in the writeup.
- As a measure of convergence, we see how quickly the average residual decreases through phases of MapReduce.
- For specific details, see SimplePageRank.java for comments. 

Blocked PageRank
- Mappers and reducers both have the following input/output types 
- Key is block number of type Text
- Value is a string (type Text) containing a node number, which block the node came from (if applicable), the PageRank of the node, and the lists of outlink nodes and their corresponding blocks, all delimited by "_".
- Map input: 
  <b(u);u_-1_PR(u)_{b(v)~v | u->v}>
- Map outputs/ Reducer input: 
  <b(u);u_-1_PR(u)_{b(v)~v | u->v}>, <b(v);v_b(u)_PR(u)/N_-1>
- Reducer output: 
  <b(u);u_-1_PR(u)_{b(v)~v | u->v}>
- Parsing and constructing the string value fields is done the same way as in Simple PageRank.
- Two Counters: one for the residuals, and another for the number of PageRank iterations per reduce task.

Blocked PageRank Functionality
- The map function is similar to that of Simple PageRank with different input/output parameters.
- The reduce function performs multiple PageRank iterations for a given block until the residual error falls below a threshold.
- Termination condition: After each MapReduce phase, take the average residual across all the blocks. If the average residual falls below an error bound (0.001 in our case), the PageRanks have converged accurately and MapReduces terminates.
- For specific details, see BlockedPageRank.java for comments. 

Random Partitions
- To partition our nodes into random blocks, we hash node numbers accordingly:
  - Block# = (Node# * 13) mod 68, where 68 = # of blocks.

Filtering edges.txt
- netid used: jkf49
- rejectMin: 0.99 * 0.94 = 0.9306
- rejectLimit: rejectMin + 0.01 = 0.9406
- number of edges actually selected: 679,761
