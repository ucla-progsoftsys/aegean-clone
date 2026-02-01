config = {
    'node1': {'type': 'client', 'next': ('node4', 'node5', 'node6')},
    'node2': {'type': 'client', 'next': ('node4', 'node5', 'node6')},
    'node3': {'type': 'client', 'next': ('node4', 'node5', 'node6')},
    'node4': {'type': 'shim', 'next': ('node7', 'node8', 'node9')},
    'node5': {'type': 'shim', 'next': ('node7', 'node8', 'node9')},
    'node6': {'type': 'shim', 'next': ('node7', 'node8', 'node9')},
    'node7': {'type': 'exec'},
    'node8': {'type': 'exec'},
    'node9': {'type': 'exec'},
}