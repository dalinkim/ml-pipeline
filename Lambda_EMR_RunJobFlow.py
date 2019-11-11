import boto3
import os

def lambda_handler(event, context):
    client = boto3.client('emr', region_name='us-east-2')

    response = client.run_job_flow(
        Name=os.environ['emr_cluster_name'],
        LogUri=os.environ['emr_log_uri'],
        ReleaseLabel='emr-5.27.0',
        Instances={
            'MasterInstanceType': os.environ['emr_instance_type'],
            'SlaveInstanceType': os.environ['emr_instance_type'],
            'InstanceCount': os.environ['emr_instance_count'],
            'Ec2KeyName': os.environ['emr_ec2_key_name'],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
        },
        Steps=[
            {
                'Name': 'Spark Application',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'client',
                        '--master', 'yarn',
                        '--class', os.environ['spark_app_ETL_main_class'],
                        os.environ['spark_app_ETL_location']
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
                        '--class', os.environ['spark_app_ML_main_class'],
                        os.environ['spark_app_ML_location']
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
        JobFlowRole=os.environ['emr_job_flow_role'],
        ServiceRole=os.environ['emr_service_role'],
    )
    return "Started an EMR cluster {}".format(response)
