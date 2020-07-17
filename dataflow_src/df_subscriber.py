#!/usr/bin/python

import os, sys
import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

logging.info(">>>>>>>>>>> Start <<<<<<<<<<<")
import time
from google.cloud import pubsub
import argparse
import json
import base64
import httplib2

parser = argparse.ArgumentParser(description='DF Message Subscriber')

parser.add_argument('--service_account',required=True,help='publisher service_acount credentials file')
parser.add_argument('--pubsub_project_id',required=True, help='subscriber PubSub project')

parser.add_argument('--pubsub_subscription',required=True, help='pubsub_subscription to pull message')

args = parser.parse_args()

scope='https://www.googleapis.com/auth/pubsub'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.service_account

pubsub_project_id = args.pubsub_project_id

PUBSUB_SUBSCRIPTION = args.pubsub_subscription

subscriber = pubsub.SubscriberClient()
subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
    project_id=pubsub_project_id,
    sub=PUBSUB_SUBSCRIPTION,
)


def callback(message):

  logging.debug("********** Start PubsubMessage ")
  #logging.info('Received message ID: {}'.format(message.message_id))
  logging.info(message.data)
  logging.debug("********** End PubsubMessage ")
  message.ack()   

subscriber.subscribe(subscription_name, callback=callback)

logging.info('Listening for messages on {}'.format(subscription_name))
while True:
  time.sleep(10)
logging.info(">>>>>>>>>>> END <<<<<<<<<<<")
