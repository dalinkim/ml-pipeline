import boto3
import os

"""
Lambda handler to spin up an EMR cluster with step executions
"""
def lambda_handler(event, context):
    # Configuration Details for EMR Cluster
    region = os.environ['REGION']
    emr_cluster_name = os.environ['EMR_CLUSTER_NAME']
    emr_log_uri = os.environ['EMR_LOG_URI']
    emr_master_instance_type = os.environ['EMR_MASTER_INSTANCE_TYPE']
    emr_master_instance_count = int(os.environ['EMR_MASTER_INSTANCE_COUNT'])
    emr_core_instance_type = os.environ['EMR_CORE_INSTANCE_TYPE']
    emr_core_instance_count = int(os.environ['EMR_CORE_INSTANCE_COUNT'])
    emr_ec2_key_name = os.environ['EMR_EC2_KEY_NAME']
    emr_job_flow_role = os.environ['EMR_JOB_FLOW_ROLE']
    emr_service_role = os.environ['EMR_SERVICE_ROLE']

    # Configuration Details for Spark Data Processor Application
    spark_data_processor_main_class = os.environ['SPARK_DATA_PROCESSOR_MAIN_CLASS']
    spark_data_processor_location = os.environ['SPARK_DATA_PROCESSOR_LOCATION']
    source_data_location = os.environ['SOURCE_DATA_LOCATION']
    transformed_data_destination_folder = os.environ['TRANSFORMED_DATA_DESTINATION_FOLDER']

    # Configuration Details for Spark Model Builder Application
    spark_model_builder_main_class = os.environ['SPARK_MODEL_BUILDER_MAIN_CLASS']
    spark_model_builder_location = os.environ['SPARK_MODEL_BUILDER_LOCATION']
    transformed_data_location = os.environ['TRANSFORMED_DATA_LOCATION']
    single_diagnosis_category = os.environ['SINGLE_DIAGNOSIS_CATEGORY']
    sagemaker_role_arn = os.environ['SAGEMAKER_ROLE_ARN']
    sagemaker_bucket_name = os.environ['SAGEMAKER_BUCKET_NAME']
    sagemaker_training_instance_type = os.environ['SAGEMAKER_TRAINING_INSTANCE_TYPE']
    sagemaker_training_instance_count = os.environ['SAGEMAKER_TRAINING_INSTANCE_COUNT']
    sagemaker_endpoint_instance_type = os.environ['SAGEMAKER_ENDPOINT_INSTANCE_TYPE']
    sagemaker_endpoint_instance_initial_count = os.environ['SAGEMAKER_ENDPOINT_INSTANCE_INITIAL_COUNT']

    client = boto3.client('emr', region_name=region)

    response = client.run_job_flow(
        Name = emr_cluster_name,
        LogUri = emr_log_uri,
        ReleaseLabel = 'emr-5.27.0',
        Instances = {
            'InstanceGroups': [
                {
                    'Name': 'Master Instance Group',
                    'Market': 'SPOT',
                    'InstanceRole': 'MASTER',
                    # 'BidPrice': using default (max on-demand),
                    'InstanceType': emr_master_instance_type,
                    'InstanceCount': emr_master_instance_count,
                },
                {
                    'Name': 'Core Instance Group',
                    'Market': 'SPOT',
                    'InstanceRole': 'CORE',
                    # 'BidPrice': using default (max on-demand),
                    'InstanceType': emr_core_instance_type,
                    'InstanceCount': emr_core_instance_count,
                },
            ],
            'Ec2KeyName': emr_ec2_key_name,
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        Steps = [
            {
                'Name': 'Spark Application',
                'ActionOnFailure': 'TERMINATE_CLUSTER', # cluster should terminate if processing fails
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'client',
                        '--master', 'yarn',
                        '--class', spark_data_processor_main_class,
                        spark_data_processor_location, # arg1
                        source_data_location, # arg2
                        transformed_data_destination_folder # arg2
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
                        '--class', spark_model_builder_main_class,
                        spark_model_builder_location,
                        transformed_data_location, # arg1
                        single_diagnosis_category, # arg2
                        sagemaker_role_arn, # arg3
                        sagemaker_bucket_name, # arg4
                        sagemaker_training_instance_type, # arg5
                        sagemaker_training_instance_count, # arg6
                        sagemaker_endpoint_instance_type, # arg7
                        sagemaker_endpoint_instance_initial_count # arg8
                    ]
                }
            }
        ],
        Applications = [
            {
                'Name': 'Spark',
            },
            {
                'Name': 'Hadoop',
            },
        ],
        VisibleToAllUsers = True,
        JobFlowRole = emr_job_flow_role,
        ServiceRole = emr_service_role,
    )

    return "Started cluster {}".format(response)
