from node import Node
from net import send_message
from quorum import QuorumHelper
import time, threading, logging

logger = logging.getLogger(__name__)

class Exec(Node):
    def __init__(self, name, host, port):
        super().__init__(name, host, port)

    def start(self):
        super().start()

    def handle_message(self, payload):
        logger.debug(f"Handler called on {self.name} with payload: {payload}")