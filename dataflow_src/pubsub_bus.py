

from __future__ import absolute_import

import argparse
import logging
import re

import six

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter

from  apache_beam.io.gcp.pubsub  import PubsubMessage


class DecryptDoFn(beam.DoFn):

  cache = None
  crypto_keys = None

  def __init__(self,mode='decrypt'):
    self.mode = mode

  def decrypt(self, attributes, data):

    import os
    import time
    import json
    import base64
    import httplib2
    from apiclient.discovery import build
    from oauth2client.client import GoogleCredentials
    from expiringdict import ExpiringDict
    from gcp_encryption.utils import AESCipher, HMACFunctions, RSACipher

    if self.cache is None:
      self.cache = ExpiringDict(max_len=100, max_age_seconds=200)
      
    scope='https://www.googleapis.com/auth/cloudkms https://www.googleapis.com/auth/pubsub'
    
    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
      credentials = credentials.create_scoped(scope)

    http = httplib2.Http()
    credentials.authorize(http)

    self.tenantID = 'tenantKey'

    if self.crypto_keys is None:
      kms_client = build('cloudkms', 'v1')
      self.crypto_keys = kms_client.projects().locations().keyRings().cryptoKeys()

    logging.info("********** Start PubsubMessage ")
    logging.debug('Received message attributes["kms_key"]: {}'.format(attributes['kms_key']))
    logging.debug('Received message attributes["dek_wrapped"]: {}'.format(attributes['dek_wrapped']))
    dek_wrapped = attributes['dek_wrapped']
    name = attributes['kms_key']

    try:
      dek = self.cache[dek_wrapped]
      logging.info("DEK Cache HIT: " + dek_wrapped)
    except KeyError:
      logging.info("DEK Cache MISS: " + dek_wrapped)
      logging.info("Starting KMS decryption API call")
      request = self.crypto_keys.decrypt(
            name=name,
            body={
            'ciphertext': (dek_wrapped).decode('utf-8'),
            'additionalAuthenticatedData': base64.b64encode(self.tenantID).decode('utf-8')
            })
      response = request.execute()
      dek=  base64.b64decode(response['plaintext'])        
      logging.info("End KMS decryption API call")
      logging.debug('Received aes_encryption_key : {}'.format( base64.b64encode(dek)))
          
      self.cache[dek_wrapped] = dek
    logging.debug("Starting AES decryption")
    ac = AESCipher(dek)
    decrypted_data = ac.decrypt(data)
    logging.debug("End AES decryption")
    logging.info('Decrypted data ' + decrypted_data)
    logging.debug("********** End PubsubMessage ")    
    return decrypted_data


  def verify(self,attributes, data):

    import os
    import time
    import json
    import base64
    import httplib2
    from apiclient.discovery import build
    from oauth2client.client import GoogleCredentials
    from expiringdict import ExpiringDict
    from gcp_encryption.utils import AESCipher, HMACFunctions, RSACipher

    if self.cache is None:
      self.cache = ExpiringDict(max_len=100, max_age_seconds=200)
      
    scope='https://www.googleapis.com/auth/cloudkms https://www.googleapis.com/auth/pubsub'
    
    credentials = GoogleCredentials.get_application_default()
    if credentials.create_scoped_required():
      credentials = credentials.create_scoped(scope)

    http = httplib2.Http()
    credentials.authorize(http)

    self.tenantID = 'tenantKey'

    if self.crypto_keys is None:
      kms_client = build('cloudkms', 'v1')
      self.crypto_keys = kms_client.projects().locations().keyRings().cryptoKeys()

    logging.info("********** Start PubsubMessage ")
    logging.debug('Received message attributes["kms_key"]: {}'.format(attributes['kms_key']))
    logging.debug('Received message attributes["sign_key_wrapped"]: {}'.format(attributes['sign_key_wrapped']))
    logging.debug('Received message attributes["signature"]: {}'.format(attributes['signature']))

    signature = attributes['signature']
    name = attributes['kms_key']
    sign_key_wrapped = attributes['sign_key_wrapped']

    try:
      unwrapped_key = self.cache[sign_key_wrapped]
      logging.info("DEK Cache HIT: " + sign_key_wrapped)
    except KeyError:
      logging.info("DEK Cache MISS: " + sign_key_wrapped)
      logging.info("Starting KMS decryption API call")
      request = self.crypto_keys.decrypt(
            name=name,
            body={
            'ciphertext': (sign_key_wrapped).decode('utf-8'),
            'additionalAuthenticatedData': base64.b64encode(self.tenantID).decode('utf-8')
            })
      response = request.execute()
      unwrapped_key =  base64.b64decode(response['plaintext'])
      logging.info("End KMS decryption API call")
      logging.debug("Verify message: " + data)
      logging.debug('  With HMAC: ' + signature)
      logging.debug('  With unwrapped key: ' + base64.b64encode(unwrapped_key))      
      self.cache[sign_key_wrapped] = unwrapped_key
          
    hh = HMACFunctions(base64.b64encode(unwrapped_key))
    sig = hh.hash(data)
    logging.debug("********** End PubsubMessage ")

    if (hh.verify(base64.b64decode(sig))):
      logging.info("Message authenticity verified")
      return data
    else:
      logging.error("ERROR: Unable to verify message ")
      return None

  def process(self, element):
    if (self.mode == 'decrypt'):
      return [self.decrypt(element.attributes, element.data)]
    if (self.mode == 'verify'):
      return [self.verify(element.attributes, element.data)]

class TermCounterDoFn(beam.DoFn):

  def __init__(self, term):
    super(TermCounterDoFn, self).__init__()
    self.term = term
    self.words_counter = Metrics.counter(self.__class__, 'words')
    self.word_lengths_counter = Metrics.counter(self.__class__, 'word_lengths')
    self.word_lengths_dist = Metrics.distribution(
        self.__class__, 'word_len_dist')
    self.empty_line_counter = Metrics.counter(self.__class__, 'empty_lines')

  def process(self, element):
    text_line = element.strip()
    if not text_line:
      self.empty_line_counter.inc(1)
    words = re.findall(r'[\w\']+', text_line, re.UNICODE)
    for w in words:
      self.words_counter.inc()
      self.word_lengths_counter.inc(len(w))
      self.word_lengths_dist.update(len(w))
    return words

def run(argv=None):
  """Build and run the pipeline."""
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--output_topic', required=True,
      help=('Output PubSub topic of the form '
            '"projects/<PROJECT>/topic/<TOPIC>".'))
  parser.add_argument(
      '--input_subscription',
      help=('Input PubSub subscription of the form '
            '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'), required=True)
  parser.add_argument(
      '--mode',
      help=("Operation mode:  must be either decrypt or verify"), required=True,
      choices=['decrypt', 'verify'])            
  known_args, pipeline_args = parser.parse_known_args(argv)

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  pipeline_options.view_as(StandardOptions).streaming = True
  p = beam.Pipeline(options=pipeline_options)

  # Read from PubSub into a PCollection.
  if known_args.input_subscription:
    messages = (p
                | beam.io.ReadFromPubSub(
                    subscription=known_args.input_subscription,with_attributes=True)
                .with_output_types(PubsubMessage))
  else:
    messages = (p
                | beam.io.ReadFromPubSub(topic=known_args.input_topic, with_attributes=True)
                .with_output_types(PubsubMessage))

  lines = messages | 'decode' >>   beam.ParDo(DecryptDoFn(mode=known_args.mode))

  # Count the occurrences of each word.
  def count_ones(word_ones):
    (word, ones) = word_ones
    return (word, sum(ones))

  counts = (lines
            | 'split' >> (beam.ParDo(TermCounterDoFn('sit'))
                          .with_output_types(six.text_type))
            | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
            | beam.WindowInto(window.FixedWindows(15, 0))
            | 'group' >> beam.GroupByKey()
            | 'count' >> beam.Map(count_ones))

  # Format the counts into a PCollection of strings.
  def format_result(word_count):
    (word, count) = word_count
    return '%s: %d' % (word, count)

  output = (counts
            | 'format' >> beam.Map(format_result)
            | 'encode' >> beam.Map(lambda x: x.encode('utf-8'))
            .with_output_types(six.binary_type))

  # Write to PubSub.
  # pylint: disable=expression-not-assigned
  output | beam.io.WriteToPubSub(known_args.output_topic)

  result = p.run()
  result.wait_until_finish()


if __name__ == '__main__':
  print("Starting")
  logging.getLogger().setLevel(logging.INFO)
  run()
