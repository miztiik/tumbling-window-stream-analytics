from aws_cdk import core
from aws_cdk import aws_kinesis as _kinesis
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_iam as _iam
from aws_cdk import aws_logs as _logs


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


class ServerlessKinesisProducerStack(core.Stack):

    def __init__(
        self,
        scope: core.Construct,
        construct_id: str,
        stack_log_level: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Add your stack resources below):
        # Create Kinesis Data Stream
        self.data_pipe_stream = _kinesis.Stream(
            self,
            "dataPipeStream",
            retention_period=core.Duration.hours(24),
            shard_count=1,
            stream_name="data_pipe"
        )

        ########################################
        #######                          #######
        #######   Stream Data Producer   #######
        #######                          #######
        ########################################

        # Read Lambda Code
        try:
            with open("tumbling_window_stream_analytics/stacks/back_end/lambda_src/stream_data_producer.py",
                      encoding="utf-8",
                      mode="r"
                      ) as f:
                data_producer_fn_code = f.read()
        except OSError:
            print("Unable to read Lambda Function Code")
            raise

        data_producer_fn = _lambda.Function(
            self,
            "streamDataProducerFn",
            function_name=f"data_producer_fn",
            description="Produce streaming data events and push to Kinesis stream",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.InlineCode(
                data_producer_fn_code),
            handler="index.lambda_handler",
            timeout=core.Duration.seconds(60),
            reserved_concurrent_executions=1,
            environment={
                "LOG_LEVEL": "INFO",
                "STREAM_NAME": f"{self.data_pipe_stream.stream_name}",
                "APP_ENV": "Production",
                "STREAM_AWS_REGION": f"{core.Aws.REGION}"
            }
        )

        # Grant our Lambda Producer privileges to write to Kinesis Data Stream
        self.data_pipe_stream.grant_read_write(data_producer_fn)

        # Create Custom Loggroup for Producer
        data_producer_lg = _logs.LogGroup(
            self,
            "dataProducerLogGroup",
            log_group_name=f"/aws/lambda/{data_producer_fn.function_name}",
            removal_policy=core.RemovalPolicy.DESTROY,
            retention=_logs.RetentionDays.ONE_DAY
        )

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
            "streamDataProducer",
            value=f"https://console.aws.amazon.com/lambda/home?region={core.Aws.REGION}#/functions/{data_producer_fn.function_name}?tab=code",
            description="Produce streaming data events and push to Kinesis stream."
        )

    # properties to share with other stacks
    @property
    def get_stream(self):
        return self.data_pipe_stream
