### Model-Builder

The model-builder application handles the additional transformation of data to be used for machine learning modeling and calls the SageMaker service to train, build, and deploy the model. 
Feature engineering work such as handling missing values and/or outliers and transforming categorical variables are done here prior to the training.  

For the NIS Core File, the model-builder application filters records by user-defined diagnosis, removes records with missing/invalid data, chooses pre-selected features (based on correlation from the notebook example), and transforms the categorical variables for feature engineering work. 
Lastly, SageMaker is called to train, build, and deploy the model.

- <a href = "https://github.com/dalinkim/ml-pipeline/blob/master/model-builder/src/main/scala/edu/uwm/cs/ModelBuilder.scala">ModelBuilder</a>: application's entry point 
- <a href = "https://github.com/dalinkim/ml-pipeline/blob/master/model-builder/src/main/scala/edu/uwm/cs/NISModelBuildingService.scala">NISModelBuildingService</a>: prepares data, builds and deploys a model
- <a href = "https://github.com/dalinkim/ml-pipeline/blob/master/model-builder/src/main/scala/edu/uwm/cs/NISPipelineBuilder.scala">NISPipelineBuilder</a>: builds Spark Pipeline including SageMaker estimator
- <a href = "https://github.com/dalinkim/ml-pipeline/blob/master/model-builder/src/main/scala/edu/uwm/cs/SageMakerTransformer.scala">SageMakerTransformer</a>: transform final DataFrame for SageMaker

<br>
The model-builder application takes following 8 arguments: 

- dataSourceFilePath: directory where all the transformed data are read (i.e. s3://my-ml-pipeline/transformed-csv/*.csv)
- diagnosis: ICD-10-CM (International Classification of Diseases, 10th Revision, Clinical Modification) code of an interested diagnosis
- sageMakerRoleArn
- sageMakerBucketName
- sageMakerTrainingInstanceType
- sageMakerTrainingInstanceCount
- sageMakerEndpointInstanceType
- sageMakerEndpointInitialInstanceCount
