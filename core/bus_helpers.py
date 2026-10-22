"""
Хелперы шины ZMQ.

Проблема, которую это решает: подписчики (SUB) используют пару вызовов
recv_string() + recv_json(). Если в очередь сокета попадает «осиротевший»
кадр (например, от leftover-процесса прошлого запуска или тестера, который
послал только заголовок-топик без payload), пары кадров рассинхронизируются:
подписчик начинает читать topic там, где лежит тело, и vice versa —
в логах это выглядит как "Expecting value: line 1 column 1".

Решение: принимать сообщение АТОМАРНО через recv_multipart(copy=False).
Либо приходит полный кадр [topic, payload] — тогда всё хорошо, либо неполный
— тогда он безопасно отбрасывается целиком, и рассинхрона не происходит.
"""
import json
import zmq


def recv_topic_payload(sub_socket, timeout_ms: int = 500):
    """Атомарно принимает одно сообщение [topic, payload].

    Возвращает (topic: str, payload: dict) либо None, если:
      - таймаут (нет сообщений);
      - кадр неполный / битый (пустой topic, нет payload, невалидный JSON).

    Битые кадры отбрасываются полностью, поэтому очередь сокета никогда
    не «съезжает» относительно границ сообщений.
    """
    if not sub_socket.poll(timeout_ms, zmq.POLLIN):
        return None
    frames = sub_socket.recv_multipart(copy=False)
    # Ожидаем ровно 2 кадра: [topic, json-payload]
    if len(frames) < 2:
        return None
    try:
        topic = frames[0].bytes.decode("utf-8")
    except Exception:
        return None
    if not topic:
        return None
    try:
        payload = json.loads(frames[1].bytes.decode("utf-8"))
    except Exception:
        return None
    return topic, payload
