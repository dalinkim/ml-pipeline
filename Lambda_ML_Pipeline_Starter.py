import boto3
import os

"""
Lambda handler to spin up an EMR cluster with step executions
"""
def lambda_handler(event, context):
    client = boto3.client('emr', region_name='us-east-2')

    response = client.run_job_flow(
        Name=os.environ['EMR_CLUSTER_NAME'],
        LogUri=os.environ['EMR_LOG_URI'],
        ReleaseLabel='emr-5.27.0',
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master Instance Group',
                    'Market': 'SPOT',
                    'InstanceRole': 'MASTER',
                    # 'BidPrice': using default (max on-demand),
                    'InstanceType': os.environ['EMR_MASTER_INSTANCE_TYPE'],
                    'InstanceCount': int(os.environ['EMR_MASTER_INSTANCE_COUNT']),
                },
                {
                    'Name': 'Core Instance Group',
                    'Market': 'SPOT',
                    'InstanceRole': 'CORE',
                    # 'BidPrice': using default (max on-demand),
                    'InstanceType': os.environ['EMR_CORE_INSTANCE_TYPE'],
                    'InstanceCount': int(os.environ['EMR_CORE_INSTANCE_COUNT']),
                },
            ],
            'Ec2KeyName': os.environ['EMR_EC2_KEY_NAME'],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        Steps=[
            {
                'Name': 'Spark Application',
                'ActionOnFailure': 'TERMINATE_CLUSTER', # cluster should terminate if processing fails
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'client',
                        '--master', 'yarn',
                        '--class', os.environ['SPARK_DATA_PROCESSOR_MAIN_CLASS'],
                        os.environ['SPARK_DATA_PROCESSOR_LOCATION'], # arg1
                        os.environ['SOURCE_DATA_LOCATION'], # arg2
                        os.environ['TRANSFORMED_DATA_DESTINATION_FOLDER'] # arg2
                    ]
                }
            },
            {
                'Name': 'Spark Application',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'client',
                        '--master', 'yarn',
                        '--class', os.environ['SPARK_MODEL_BUILDER_MAIN_CLASS'],
                        os.environ['SPARK_MODEL_BUILDER_LOCATION'],
                        os.environ['TRANSFORMED_DATA_LOCATION'], # arg1
                        os.environ['SINGLE_DIAGNOSIS_CATEGORY'], # arg2
                        os.environ['SAGEMAKER_ROLE_ARN'], # arg3
                        os.environ['SAGEMAKER_BUCKET_NAME'], # arg4
                        os.environ['SAGEMAKER_TRAINING_INSTANCE_TYPE'], # arg5
                        os.environ['SAGEMAKER_TRAINING_INSTANCE_COUNT'], # arg6
                        os.environ['SAGEMAKER_ENDPOINT_INSTANCE_TYPE'], # arg7
                        os.environ['SAGEMAKER_ENDPOINT_INSTANCE_COUNT'] # arg8
                    ]
                }
            }
        ],
        Applications=[
            {
                'Name': 'Spark',
            },
            {
                'Name': 'Hadoop',
            },
        ],
        VisibleToAllUsers=True,
        JobFlowRole=os.environ['EMR_JOB_FLOW_ROLE'],
        ServiceRole=os.environ['EMR_SERVICE_ROLE'],
    )
    return "Started cluster {}".format(response)
