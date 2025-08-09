# node1/pubsub.py
class PubSub:
    def __init__(self):
        self.subscribers = []

    def subscribe(self, callback):
        self.subscribers.append(callback)

    def publish(self, fragment_name):
        for callback in self.subscribers:
            callback(fragment_name)
