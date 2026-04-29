import zmq
from loguru import logger

class BusManager:
    def __init__(self, pub_port: int, rep_port: int):
        self.context = zmq.Context()
        self.pub_port = pub_port
        self.rep_port = rep_port
        
        self.pub_socket = None
        self.rep_socket = None

    def start(self):
        try:
            # PUB для данных (рассылка всем)
            self.pub_socket = self.context.socket(zmq.PUB)
            self.pub_socket.bind(f"tcp://*:{self.pub_port}")
            logger.info(f" PUB Socket bound on port {self.pub_port}")

            # REP для команд (ответы на запросы)
            self.rep_socket = self.context.socket(zmq.REP)
            self.rep_socket.bind(f"tcp://*:{self.rep_port}")
            logger.info(f"📥 REP Socket bound on port {self.rep_port}")
            
            return True
        except Exception as e:
            logger.error(f"❌ ZMQ Bus Error: {e}")
            return False

    def stop(self):
        if self.pub_socket:
            self.pub_socket.setsockopt(zmq.LINGER, 0)
            self.pub_socket.close()
        if self.rep_socket:
            self.rep_socket.setsockopt(zmq.LINGER, 0)
            self.rep_socket.close()
        self.context.term()
        logger.info("🛑 Bus Manager stopped")