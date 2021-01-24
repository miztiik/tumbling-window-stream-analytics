#!/usr/bin/env python3


#!/usr/bin/env python3

from aws_cdk import core

from tumbling_window_stream_analytics.stacks.back_end.tumbling_window_stream_analytics_stack import TumblingWindowStreamAnalyticsStack
from tumbling_window_stream_analytics.stacks.back_end.serverless_kinesis_producer_stack import ServerlessKinesisProducerStack

app = core.App()


# Kinesis Data Producer on Lambda
serverless_kinesis_producer_stack = ServerlessKinesisProducerStack(
    app,
    f"{app.node.try_get_context('project')}-kinesis-producer-stack",
    stack_log_level="INFO",
    description="Miztiik Automation: Kinesis Data Producer on Lambda"
)
# Analytics on stream of data using lambda tumbling window.
tumbling_window_stream_analytics_stack = TumblingWindowStreamAnalyticsStack(
    app,
    f"{app.node.try_get_context('project')}-stack",
    stack_log_level="INFO",
    data_pipe_stream=serverless_kinesis_producer_stack.get_stream,
    description="Miztiik Automation: Analytics on stream of data using lambda tumbling window"
)


# Stack Level Tagging
_tags_lst = app.node.try_get_context("tags")

if _tags_lst:
    for _t in _tags_lst:
        for k, v in _t.items():
            core.Tags.of(app).add(k, v, apply_to_launched_instances=True)


app.synth()
