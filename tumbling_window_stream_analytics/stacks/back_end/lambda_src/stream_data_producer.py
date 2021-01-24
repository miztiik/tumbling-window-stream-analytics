# -*- coding: utf-8 -*-
"""
.. module: sample_kinesis_producer
    :Actions: Put Records in Kinesis Data Stream 
    :copyright: (c) 2020 Mystique.,
.. moduleauthor:: Mystique
.. contactauthor:: miztiik@github issues
"""


import json
import logging
import os
import random
import string
import time
import uuid

import boto3

__author__ = "Mystique"
__email__ = "miztiik@github"
__version__ = "0.0.1"
__status__ = "production"


class GlobalArgs:
    """ Global statics """
    OWNER = "Mystique"
    ENVIRONMENT = "production"
    MODULE_NAME = "stream_record_producer"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    STREAM_NAME = os.getenv("STREAM_NAME", "data_pipe")
    STREAM_AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def set_logging(lv=GlobalArgs.LOG_LEVEL):
    """ Helper to enable logging """
    logging.basicConfig(level=lv)
    logger = logging.getLogger()
    logger.setLevel(lv)
    return logger


logger = set_logging()


def _gen_uuid():
    """ Generates a uuid string and return it """
    return str(uuid.uuid4())


def send_data(client, data, key, stream_name):
    logger.info(f"data:{json.dumps(data)}")
    # logger.info(f"key:{key}")
    resp = client.put_records(
        Records=[
            {
                "Data": json.dumps(data),
                "PartitionKey": key},
        ],
        StreamName=stream_name

    )
    logger.info(f"Response:{resp}")


client = boto3.client(
    "kinesis", region_name=GlobalArgs.STREAM_AWS_REGION)


def lambda_handler(event, context):
    resp = {"status": False}
    logger.info(f"Event: {json.dumps(event)}")

    _random_user_name = ["Aarakocra", "Aasimar", "Beholder", "Bugbear", "Centaur", "Changeling", "Deep Gnome", "Deva", "Dragonborn", "Drow", "Dwarf", "Eladrin", "Elf", "Firbolg", "Genasi", "Githzerai", "Gnoll", "Gnome", "Goblin", "Goliath", "Hag", "Half-Elf",
                         "Half-Orc", "Halfling", "Hobgoblin", "Kalashtar", "Kenku", "Kobold", "Lizardfolk", "Loxodon", "Mind Flayer", "Minotaur", "Orc", "Shardmind", "Shifter", "Simic Hybrid", "Tabaxi", "Tiefling", "Tortle", "Triton", "Vedalken", "Warforged", "Wilden", "Yuan-Ti"]

    try:
        record_count = 0
        tot_sales = 0
        for i in range(random.randint(1, 300)):
            _s = random.randint(1, 500)
            send_data(
                client,
                {"cust_name": random.choice(_random_user_name),
                 "cust_id": random.randint(19810313, 20210116),
                 "sales": _s
                 },
                _gen_uuid(),
                GlobalArgs.STREAM_NAME
            )
            record_count += 1
            tot_sales += _s
        resp["record_count"] = record_count
        resp["tot_sales"] = tot_sales
        logger.info(f"resp: {json.dumps(resp)}")
        resp["status"] = True

    except Exception as e:
        logger.error(f"ERROR:{str(e)}")
        resp["error_message"] = str(e)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": resp
        })
    }
