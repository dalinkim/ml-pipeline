### Data-Processor

The data-processor application handles initial processing of the input data. It is responsible for ingesting data and converting them to a desirable state for subsequent work. This may be especially useful if this machine learning pipeline is a subcomponent of a larger pipeline where the data continues to be processed after being used for machine learning work. 

For the NIS Core File, the data-processor application parses the data in the initial text file and transforms them into a csv format.

- `DataProcessor`: application's entry point 
- `NISDataProcessingService`: processes data and saves to S3
- `NISDataParser`: parses data from raw text file according to the file specification.