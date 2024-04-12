#
# (C) Copyright Cloudlab URV 2020
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pika
import os
import sys
import uuid
import json
import logging
from functools import partial

from lithops.version import __version__
from lithops.utils import setup_lithops_logger, b64str_to_dict
from lithops.worker import function_handler
from lithops.worker.utils import get_runtime_metadata
from lithops.constants import JOBS_PREFIX
from lithops.storage.storage import InternalStorage


logger = logging.getLogger('lithops.worker')


def extract_runtime_meta(payload):
    logger.info(f"Lithops v{__version__} - Generating metadata")

    runtime_meta = get_runtime_metadata()

    internal_storage = InternalStorage(payload)
    status_key = '/'.join([JOBS_PREFIX, payload['runtime_name'] + '.meta'])
    logger.info(f"Runtime metadata key {status_key}")
    dmpd_response_status = json.dumps(runtime_meta)
    internal_storage.put_data(status_key, dmpd_response_status)


def on_received_work(channel, method, properties, body):
    logger.info(f"Received new job from the queue")
    function_handler(json.loads(body))
    channel.basic_ack(delivery_tag=method.delivery_tag)


def start_worker(payload):
    logger.info(f"Lithops v{__version__} - Starting kubernetes execution")

    os.environ['__LITHOPS_ACTIVATION_ID'] = str(uuid.uuid4()).replace('-', '')[:12]
    os.environ['__LITHOPS_BACKEND'] = 'k8s'

    params = pika.URLParameters(payload['amqp_url'])
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=payload['queue_uuid'], durable=True)
    channel.basic_qos(prefetch_count=1)

    if payload['fixed_setup']:
        channel.basic_consume(queue=payload['queue_uuid'], on_message_callback=on_received_work)
        logger.info("Static k8s worker started...")
        channel.start_consuming()
    else:
        method, properties, body = channel.basic_get(queue=payload['queue_uuid'], auto_ack=False)
        on_received_work(channel, method, properties, body)
        logger.info("Finishing kubernetes execution")


if __name__ == '__main__':
    action = sys.argv[1]
    encoded_payload = sys.argv[2]

    payload = b64str_to_dict(encoded_payload)
    setup_lithops_logger(payload.get('log_level', 'INFO'))

    switcher = {
        'get_metadata': partial(extract_runtime_meta, payload),
        'run_job': partial(start_worker, payload)
    }

    func = switcher.get(action, lambda: "Invalid command")
    func()
