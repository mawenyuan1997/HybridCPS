import redis
import time
import threading

import random
N = 1
def producer():
    r = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host='10.0.0.1', port=6379,
            decode_responses=True, encoding='utf-8'))
    now = time.time()
    for x in range(N):
        print('Sending {0}'.format(now))
        r.publish(str(x), str(now))
        # time.sleep(1)


if __name__ == '__main__':
    producer()