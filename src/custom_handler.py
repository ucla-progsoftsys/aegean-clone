import logging

from net import send_message

logger = logging.getLogger(__name__)

def custom_handler(node, payload):
    logger.debug(f"Handler called on {node.name} with payload: {payload}")
    if node.name == 'node1':
        result = send_message('node2', '8000', payload)
        return result
    elif node.name == 'node2':
        result = send_message('node3', '8000', payload)
        return result
    elif node.name == 'node3':
        return {'value': 3}
    return payload