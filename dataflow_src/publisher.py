#!/usr/bin/python

import os ,sys
import time
import logging

from google.cloud import pubsub
from google.cloud import kms
import argparse

import json, time
import base64, binascii
import httplib2
import lorem

from expiringdict import ExpiringDict

from gcp_encryption.utils import AESCipher, HMACFunctions, RSACipher


logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

parser = argparse.ArgumentParser(description='Publish encrypted message with KMS only')
parser.add_argument('--service_account',required=True,help='publisher service_acount credentials file')
parser.add_argument('--mode',required=True, choices=['encrypt','sign'], help='mode must be encrypt or sign')
parser.add_argument('--kms_project_id',required=True, help='publisher KMS project')
parser.add_argument('--kms_location',required=True, help='KMS Location')
parser.add_argument('--kms_key_ring_id',required=True, help='KMS key_ring_id')
parser.add_argument('--kms_key_id',required=True, help='KMS keyid')
parser.add_argument('--pubsub_project_id',required=True, help='publisher projectID')
parser.add_argument('--pubsub_topic',required=True, help='pubsub_topic to publish message')
parser.add_argument('--tenantID',required=False, default="tenantKey", help='Optional additionalAuthenticatedData')
args = parser.parse_args()

scope='https://www.googleapis.com/auth/cloudkms https://www.googleapis.com/auth/pubsub'

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.service_account

pubsub_project_id = args.pubsub_project_id
kms_project_id = args.kms_project_id
location_id = args.kms_location
key_ring_id = args.kms_key_ring_id
crypto_key_id = args.kms_key_id
tenantID = args.tenantID

PUBSUB_TOPIC=args.pubsub_topic

cache = ExpiringDict(max_len=100, max_age_seconds=200)

name = 'projects/{}/locations/{}/keyRings/{}/cryptoKeys/{}'.format(
        kms_project_id, location_id, key_ring_id, crypto_key_id)

publisher = pubsub.PublisherClient()
kmsclient = kms.KeyManagementServiceClient()

if args.mode =="sign":
  logging.info(">>>>>>>>>>> Start Sign with with locally generated key. <<<<<<<<<<<")

  for i in range(10):

    if (i%5 ==0 ):
      logging.info("Rotating key")
      hh = HMACFunctions()
      sign_key = hh.getKey()
      logging.debug("Generated Derived Key: " + sign_key)

      logging.info("Starting KMS encryption API call")
      #sign_key_wrapped = kmsclient.encrypt(name=name, plaintext=sign_key,additional_authenticated_data=tenantID.encode('utf-8'))
      sign_key_wrapped = kmsclient.encrypt(name=name, plaintext=sign_key.encode('utf-8'),additional_authenticated_data=tenantID.encode('utf-8'))

      logging.info("End KMS encryption API call")

    cleartext_message = lorem.paragraph()
    msg_hash = hh.hash(cleartext_message.encode('utf-8'))
    logging.info("Generated Signature: " + msg_hash.decode('utf-8'))
    logging.debug("End signature")

    logging.info("Start PubSub Publish")
    publisher = pubsub.PublisherClient()
    topic_name = 'projects/{project_id}/topics/{topic}'.format(
      project_id=pubsub_project_id,
      topic=PUBSUB_TOPIC,
    )

    publisher.publish(topic_name, data=cleartext_message.encode('utf-8'), kms_key=name, sign_key_wrapped=base64.b64encode(sign_key_wrapped.ciphertext), signature=msg_hash)
    logging.info("Published Message: " + str(cleartext_message))
    logging.info(" with key_id: " + name)
    logging.info(" with wrapped signature key " + base64.b64encode(sign_key_wrapped.ciphertext).decode())
    time.sleep(5)
  logging.debug("End PubSub Publish")
  logging.info(">>>>>>>>>>> END <<<<<<<<<<<")

if args.mode =="encrypt":
    logging.info(">>>>>>>>>>> Start Encryption with locally generated key.  <<<<<<<<<<<")

    for i in range(10):

      if (i%5 ==0 ):
        logging.info("Rotating symmetric key")


        ac = AESCipher(encoded_key=None)
        dek = ac.getKey().encode()

        logging.debug("Generated dek: " + base64.b64encode(dek).decode() )


        logging.info("Starting KMS encryption API call")

        dek_encrypted = kmsclient.encrypt(name=name, plaintext=dek,additional_authenticated_data=tenantID.encode('utf-8'))

        dek_key_wrapped = dek_encrypted.ciphertext
        logging.info("Wrapped dek: " +  base64.b64encode(dek_key_wrapped).decode('utf-8'))
        logging.info("End KMS encryption API call")

        logging.debug("Starting AES encryption")

      cleartext_message = lorem.paragraph()
      encrypted_message = ac.encrypt(cleartext_message.encode('utf-8'),associated_data="")

      logging.debug("End AES encryption")
      logging.debug("Encrypted Message with dek: " + encrypted_message)

      logging.info("Start PubSub Publish")
      
      topic_name = 'projects/{project_id}/topics/{topic}'.format(
          project_id=pubsub_project_id,
          topic=PUBSUB_TOPIC,
      )
      publisher.publish(topic_name, data=encrypted_message.encode(), kms_key=name, dek_wrapped=base64.b64encode(dek_encrypted.ciphertext).decode())
      logging.info("Published Message: " + encrypted_message)
      time.sleep(5)
    logging.info("End PubSub Publish")
    logging.info(">>>>>>>>>>> END <<<<<<<<<<<")
