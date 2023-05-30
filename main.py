"""Publishes multiple messages to a Pub/Sub topic with an error handler."""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), './.python'))

from concurrent import futures
from google import auth
from google.cloud import pubsub_v1
from typing import Callable

project_id = "pubsub-test-387809"
topic_id = "gc-aws-topic"

credentials, _ = auth.load_credentials_from_file("./clientLibraryConfig-aws-provider.json")

publisher = pubsub_v1.PublisherClient(credentials = credentials)
topic_path = publisher.topic_path(project_id, topic_id)
publish_futures = []

def get_callback(
    publish_future: pubsub_v1.publisher.futures.Future, data: str
) -> Callable[[pubsub_v1.publisher.futures.Future], None]:
    def callback(publish_future: pubsub_v1.publisher.futures.Future) -> None:
        try:
            # Wait 60 seconds for the publish call to succeed.
            print(publish_future.result(timeout=60))
        except futures.TimeoutError:
            print(f"Publishing {data} timed out.")

    return callback

def main():

    f = open('./senddata.txt', 'r')
    data = f.read()
    f.close()

    # When you publish a message, the client returns a future.
    publish_future = publisher.publish(topic_path, data.encode("utf-8"))
    # Non-blocking. Publish failures are handled in the callback function.
    publish_future.add_done_callback(get_callback(publish_future, data))
    publish_futures.append(publish_future)

    # Wait for all the publish futures to resolve before exiting.
    futures.wait(publish_futures, return_when=futures.ALL_COMPLETED)

print(f"Published messages with error handler to {topic_path}.")


if __name__ == "__main__":
    main()