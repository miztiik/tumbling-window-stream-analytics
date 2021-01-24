from aws_cdk import core
from aws_cdk import aws_kinesis as _kinesis
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_iam as _iam
from aws_cdk import aws_logs as _logs
from aws_cdk import aws_s3 as _s3


class GlobalArgs:
    """
    Helper to define global statics
    """

    OWNER = "MystiqueAutomation"
    ENVIRONMENT = "production"
    REPO_NAME = "tumbling-window-stream-analytics"
    SOURCE_INFO = f"https://github.com/miztiik/{REPO_NAME}"
    VERSION = "2021_01_16"
    MIZTIIK_SUPPORT_EMAIL = ["mystique@example.com", ]


class TumblingWindowStreamAnalyticsStack(core.Stack):

    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        stack_log_level,
        data_pipe_stream,
        ** kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Create an S3 Bucket for storing streaming data events
        stream_data_store = _s3.Bucket(
            self,
            "streamDataLake",
            removal_policy=core.RemovalPolicy.DESTROY,
            auto_delete_objects=True
        )

        # Defines an AWS Lambda resource):
        ###########################################
        #####      KINESIS CONSUMER 1       #######
        ###########################################

        # Read Lambda Code
        try:
            with open("tumbling_window_stream_analytics/stacks/back_end/lambda_src/stream_record_processor.py",
                      encoding="utf-8",
                      mode="r"
                      ) as f:
                data_consumer_fn_code = f.read()
        except OSError:
            print("Unable to read Lambda Function Code")
            raise

        stream_record_processor_fn = _lambda.Function(
            self,
            "streamProcessor",
            function_name=f"stream_record_processor",
            description="Perform streaming analytics using windows functions on kinesis stream data",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.InlineCode(
                data_consumer_fn_code),
            handler="index.lambda_handler",
            timeout=core.Duration.seconds(60),
            reserved_concurrent_executions=1,
            environment={
                "LOG_LEVEL": "INFO",
                "STREAM_NAME": f"{data_pipe_stream.stream_name}",
                "APP_ENV": "Production",
                "STREAM_AWS_REGION": f"{core.Aws.REGION}",
                "DATA_STORE_BKT_NAME": f"{stream_data_store.bucket_name}"
            }
        )

        # Add permissions to lambda to read Kinesis
        roleStmt1 = _iam.PolicyStatement(
            effect=_iam.Effect.ALLOW,
            resources=[f"{data_pipe_stream.stream_arn}"],
            actions=[
                "kinesis:GetRecords",
                "kinesis:GetShardIterator",
                "kinesis:ListStreams",
                "kinesis:DescribeStream",
            ]
        )
        roleStmt1.sid = "AllowKinesisToTriggerLambda"
        stream_record_processor_fn.add_to_role_policy(roleStmt1)

        # Add Kinesis Trigger for Lambda
        kinesis_evnt_trigger = _lambda.CfnEventSourceMapping(
            self,
            "dataPipeConsumer",
            enabled=True,
            function_name=f"{stream_record_processor_fn.function_name}",
            batch_size=2,
            event_source_arn=f"{data_pipe_stream.stream_arn}",
            starting_position="TRIM_HORIZON",
            tumbling_window_in_seconds=120
        )

        # stream_record_processor_fn.add_event_source_mapping(
        #     "dataPipeConsumer1",
        #     event_source_arn=f"{data_pipe_stream.stream_arn}",
        #     batch_size=2,
        #     enabled=True,
        #     starting_position=_lambda.StartingPosition.TRIM_HORIZON
        # )

        # Add permissions to lambda to write to S3
        roleStmt2 = _iam.PolicyStatement(
            effect=_iam.Effect.ALLOW,
            resources=[
                f"{stream_data_store.bucket_arn}/*"
            ],
            actions=[
                "s3:PutObject"
            ]
        )
        roleStmt2.sid = "AllowLambdaToWriteToS3"
        stream_record_processor_fn.add_to_role_policy(roleStmt2)

        ###########################################
        ################# OUTPUTS #################
        ###########################################
        output_0 = core.CfnOutput(
            self,
            "AutomationFrom",
            value=f"{GlobalArgs.SOURCE_INFO}",
            description="To know more about this automation stack, check out our github page."
        )

        output_1 = core.CfnOutput(
            self,
            "DataStore",
            value=f"https://console.aws.amazon.com/s3/buckets/{stream_data_store.bucket_name}",
            description="The datastore bucket"
        )
        output_2 = core.CfnOutput(
            self,
            "streamDataProcessor",
            value=f"https://console.aws.amazon.com/lambda/home?region={core.Aws.REGION}#/functions/{stream_record_processor_fn.function_name}",
            description="Produce streaming data events and push to Kinesis stream."
        )
