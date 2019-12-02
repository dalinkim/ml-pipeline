### Lambda-Handler

There are 2 lambdas used in this pipeline.

- ml-pipeline-starter: S3-triggered lambda which spins up an EMR cluster with step executions
- SageMaker-endpoint-invoker: API-Gateway-triggered lambda which calls SageMaker model endpoint and gets inference