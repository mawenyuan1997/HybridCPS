import redis
import time
import threading

N = 1
def zerg_rush(n):
    for x in range(n):
        t = threading.Thread(target=callback, args=(str(x),))
        # t.setDaemon(True)
        t.start()


all_lat = []
def callback(ch):
    r = redis.client.StrictRedis(connection_pool=redis.ConnectionPool(
            host='10.0.0.1', port=6379,
            decode_responses=True, encoding='utf-8'))

    sub = r.pubsub()
    sub.subscribe(ch)
    while True:
        for m in sub.listen():
            if m.get("type") == "message":
                latency = time.time() - float(m['data'])
                print('Recieved: {0}'.format(latency))
                all_lat.append(latency)

def consumer(n):
    zerg_rush(n)
    time.sleep(5)
    f = open("record.txt", "a")
    f.write(str(sum(all_lat)/len(all_lat)) + "\n")
    f.close()
    print(len(all_lat))
    print('mean latency:', sum(all_lat) / len(all_lat))
    print('max latency:', max(all_lat))


if __name__ == '__main__':
    consumer(N)