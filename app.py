#!/usr/bin/env python3

from aws_cdk import core

from tumbling_window_stream_analytics.tumbling_window_stream_analytics_stack import TumblingWindowStreamAnalyticsStack


app = core.App()
TumblingWindowStreamAnalyticsStack(app, "tumbling-window-stream-analytics")

app.synth()
