# -*- coding: utf-8 -*-
"""
.. module: stream_record_consumer
    :Actions: Process kinesis records
    :copyright: (c) 2020 Mystique.,
.. moduleauthor:: Mystique
.. contactauthor:: miztiik@github issues
"""

import json
import base64
import logging
import time
import datetime
import os
import boto3

__author__ = "Mystique"
__email__ = "miztiik@github"
__version__ = "0.0.1"
__status__ = "production"


class GlobalArgs:
    """ Global statics """
    OWNER = "Mystique"
    ENVIRONMENT = "production"
    MODULE_NAME = "stream_record_processor"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    STREAM_NAME = os.getenv("STREAM_NAME", "data_pipe")
    STREAM_AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    DATA_STORE_BKT_NAME = os.getenv("DATA_STORE_BKT_NAME")
    DATA_STORE_PREFIX = "revenue_sum"


def set_logging(lv=GlobalArgs.LOG_LEVEL):
    """ Helper to enable logging """
    logging.basicConfig(level=lv)
    logger = logging.getLogger()
    logger.setLevel(lv)
    return logger


logger = set_logging()


def write_data_to_s3(bucket_name, data):
    s3 = boto3.resource("s3")
    obj_key_name = f"{GlobalArgs.DATA_STORE_PREFIX}/dt={datetime.datetime.now().strftime('%Y_%m_%d')}/{int(time.time()*1000)}.json"
    object = s3.Object(bucket_name, obj_key_name)
    resp = object.put(Body=json.dumps(data))
    logger.info(json.dumps(resp))


def lambda_handler(event, context):
    resp = {"status": False, "records": "", "total_sales": 0}
    logger.info(f"Event: {json.dumps(event)}")
    bucket_name = GlobalArgs.DATA_STORE_BKT_NAME

    evnt_state = event.get("state")
    # Create the state object on first invocation or use state passed in
    if not evnt_state:
        evnt_state = {"sales": 0}
    logger.info(f"current_state:{evnt_state}")

    try:
        if event.get("Records"):
            for record in event["Records"]:
                # Kinesis data is base64 encoded so decode here
                payload = base64.b64decode(record["kinesis"]["data"])
                payload = json.loads(str(payload.decode("utf-8")))
                logger.info(f"decoded_to_json: {payload}")
                logger.info(f"payload_type: {type(payload)}")
                # Process records with custom aggregation logic
                if payload.get("sales"):
                    evnt_state["sales"] += payload.get("sales")
        if event.get("isFinalInvokeForWindow"):
            logger.info(f'{{"tumbling_window_end": True}}')
            write_data_to_s3(bucket_name, evnt_state)
        logger.info(f"updated_state:{evnt_state}")
        logger.info(
            f'{{"records_processed":{len(event.get("Records"))}}}')
    except Exception as e:
        logger.error(f"ERROR:{str(e)}")
        resp["error_message"] = str(e)

    # return {
    #     "statusCode": 200,
    #     "body": json.dumps({
    #         "message": resp
    #     })
    # }
    # Return the state for the next invocation
    return {"state": evnt_state}


if __name__ == "__main__":
    lambda_handler({}, {})
