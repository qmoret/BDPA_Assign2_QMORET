# BDPA

This is the second assignment for Centrale Paris's course "Big data processing and analytics", using Hadoop MapReduce and HDFS.

## Configuration used

Hadoop 2.7.3 pseudo distributed on MacOS, with native libraries installed.

## Assignment 2


### PreProcessing

Removes all StopWords, keep only unique words and non empty line.
Order the tokens in ascending order of global frequency.
Uses the output of StopWords from first assignment (in `~/StopWords/stopwords.csv`on my HDFS), and the output of the classic WordCount (in `WordCount/output/part-r-00000;)
1. Import data into HDFS, in  `~/input/`
2. Compile java file: `javac PreProcessing.java -cp $(hadoop classpath);`
3. Compile JAR : `jar cf sw.jar PreProcessing*.class`
4. Run JAR : `hadoop jar sw.jar PreProcessing input/pg100.txt PreProcessing/output StopWords/stopwords.csv WordCount/output/part-r-00000; `

### PairWise comparisons
Performs pairwise comparisons of all lines. Includes a hardcoded bound of the number of lines processed (here 5000).
1. Compile java file: `javac PairWise.java -cp $(hadoop classpath);`
2. Compile JAR : `jar cf sw.jar PairWise*.class`
3. Run JAR : `hadoop jar sw.jar PairWise PreProcessing/output PairWise/output PreProcessing/lineNumber;`


### InvertedIndex

Shows the list of lines in which words (excluding stop words) appear.

1. Compile java file: `javac InvertedIndex.java -cp $(hadoop classpath)`
2. Compile JAR : `jar cf ii.jar InvertedIndex*.class`
3. Run JAR : `hadoop jar ii.jar InvertedIndex PreProcessing/output InvertedIndex2/output;`

### SetSimilarityJoin

Outputs the pairs that are more similar threshold (here hardcoded to 0.8). See report for more details on the implementation

1. Compile java file: `javac SetSimilarityJoins.java -cp $(hadoop classpath);`
2. Compile JAR : `jar cf ii.jar SetSimilarityJoins*.class`
3. Run JAR : `hadoop jar ii.jar SetSimilarityJoins PreProcessing/output SetSimilarityJoins/output InvertedIndex2/output/part-r-00000;`
