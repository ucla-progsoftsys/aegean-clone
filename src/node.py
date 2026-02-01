import argparse
import logging
from http.server import ThreadingHTTPServer

from net import make_handler
from custom_handler import custom_handler

logger = logging.getLogger(__name__)


class Node:
    def __init__(self, name, host, port):
        self.name = name
        self.host = host
        self.port = port
        self._server = None

    def start(self):
        if self._server is not None:
            logger.debug(f"Node {self.name} already running")
            return

        logger.info(f"Starting node {self.name} on {self.host}:{self.port}")
        handler = make_handler(self.handle_message)
        self._server = ThreadingHTTPServer((self.host, self.port), handler)
        self._server.serve_forever()

    def handle_message(self, payload):
        return custom_handler(self, payload)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--name", type=str, required=True)
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    node = Node(args.name, host=args.host, port=args.port)
    node.start()
