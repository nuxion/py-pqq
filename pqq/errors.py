class EmptyQueue(Exception):
    def __init__(self, qname):
        super().__init__(f"Queue {qname} is empty")
