from geo_tracker.trace_intelligence.trace_intel import Trace
from geo_tracker.route_stats.route_stats import run_route_stats
from concurrent.futures import ThreadPoolExecutor
import logging as logger
from s3_connection import S3Connection
import pika, json
import constants as cons
import config

logger.getLogger().setLevel(logger.INFO)

connection = pika.BlockingConnection(pika.ConnectionParameters(host=cons.host))
channel = connection.channel()

channel.exchange_declare(exchange=cons.exchange,
                         exchange_type=cons.exchange_type)

channel.queue_declare(queue=cons.queue_name, durable=True)
channel.queue_bind(exchange=cons.exchange,
                   queue=cons.queue_name,
                   routing_key=cons.routing_key)

channel.queue_declare(queue=cons.publish_queue_name, durable=True)
channel.queue_bind(exchange=cons.exchange,
                   queue=cons.publish_queue_name,
                   routing_key=cons.publish_routing_key)

CLIENT = S3Connection()


def create_bucket_and_key():
    return "bucket", "key"


def add_is_middle(lm_data):
    try:
        trace_data = lm_data['trace_data']
        for trace_data_i in trace_data:
            trace_data_i['is_middle'] = 0
        return lm_data
    except Exception as e:
        logger.error("Problem with LM data: %s", e)
        return None


def callback(ch, method, properties, body):
    try:
        lm_data_repo_link = body.decode("utf-8")
        # lm_data = S3Connection._get_from_s3(CLIENT, cons.s3_bucket, cons.s3_key)
        lm_data = json.load(open(lm_data_repo_link))
        lm_data = add_is_middle(lm_data)
        obj = Trace(lm_data, 'LM', 60, config.V3_HEADERS, config.V3_GEOFENCING_URL)
        # Execute processes parallel
        logger.info("Calling get_trace_intel and run_route_stats parallel")
        pool = ThreadPoolExecutor(max_workers=2)
        trace_intel_future = pool.submit(obj.get_trace_intel)
        route_stats_future = pool.submit(run_route_stats, lm_data, config.MATCH_SERVICE_STRING,
                                         config.ROUTE_SERVICE_STRING, config.ROUTE_HEADERS)
        pool.shutdown(wait=True)

        trace_intel = trace_intel_future.result()
        trip_distance, route_stats_error, route_stats_error_reason = route_stats_future.result()
        trace_intel['meta_data']['trip_distance'] = trip_distance
        trace_intel['meta_data']['route_stats_error'] = route_stats_error
        trace_intel['meta_data']['route_stats_error_reason'] = route_stats_error_reason

        # publish_bucket_name, publish_key_name = create_bucket_and_key()
        # S3Connection._put_to_s3(CLIENT, trace_intel, publish_bucket_name, publish_key_name)
        logger.info("Publishing data to publish_queue")
        channel.basic_publish(exchange=cons.exchange,
                               routing_key=cons.publish_routing_key,
                               body=json.dumps(trace_intel),
                               properties=pika.BasicProperties(
                                   delivery_mode=2  # making msg persistant
                               ))
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Completed successfuly")

    except Exception as e:
        logger.error("Error while calling function %s", e)


channel.basic_consume(callback,
                      queue=cons.queue_name,
                      no_ack=False)
channel.start_consuming()
