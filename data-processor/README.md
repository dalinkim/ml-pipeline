### Data-Processor

The _data-processor_ application handles initial processing of the input data. It is responsible for ingesting data and converting them to a desirable state for subsequent work. This may be especially useful if this machine learning pipeline is a subcomponent of a larger pipeline where the data continues to be processed after being used for machine learning work. 

For the NIS Core File, the _data-processor_ parses records in the initial text file and transforms them into a csv format.
See <a href = "https://www.hcup-us.ahrq.gov/db/nation/nis/tools/stats/FileSpecifications_NIS_2016_Core.TXT">2016 NIS File Specification</a> for original data format detail.

- <a href = "https://github.com/dalinkim/ml-pipeline/blob/master/data-processor/src/main/scala/edu/uwm/cs/DataProcessor.scala">DataProcessor</a>: application's entry point 
- <a href = "https://github.com/dalinkim/ml-pipeline/blob/master/data-processor/src/main/scala/edu/uwm/cs/NISDataProcessingService.scala">NISDataProcessingService</a>: processes data and saves to S3
- <a href = "https://github.com/dalinkim/ml-pipeline/blob/master/data-processor/src/main/scala/edu/uwm/cs/NISDataParser.scala">NISDataParser</a>: parses data from raw text file according to the file specification.

<br>
The data-processor application takes following 2 arguments: 
- dataSourceFilePath: directory where all input files are read
- dataOutputFolderPath: directory where all output files are saved
