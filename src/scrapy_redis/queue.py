from scrapy.utils.reqser import request_to_dict, request_from_dict

from . import picklecompat
import logging
import time
from .bucket_hash import BucketHash
from scrapy.utils.request import request_fingerprint

logger = logging.getLogger(__name__)


class Base(object):
    """Per-spider base queue class"""

    logger = logger

    def __init__(self, server, spider, key, serializer=None):
        """Initialize per-spider redis queue.

        Parameters
        ----------
        server : StrictRedis
            Redis client instance.
        spider : Spider
            Scrapy spider instance.
        key: str
            Redis key where to put and get messages.
        serializer : object
            Serializer object with ``loads`` and ``dumps`` methods.

        """
        if serializer is None:
            # Backward compatibility.
            # TODO: deprecate pickle.
            serializer = picklecompat
        if not hasattr(serializer, 'loads'):
            raise TypeError("serializer does not implement 'loads' function: %r"
                            % serializer)
        if not hasattr(serializer, 'dumps'):
            raise TypeError("serializer '%s' does not implement 'dumps' function: %r"
                            % serializer)

        self.server = server
        self.spider = spider
        self.key = key % {'spider': spider.name}
        self.exist_bucket_key = self.key + "_exist_bucket"
        self.serializer = serializer
        # 对url进行分桶
        self.bucket = BucketHash(10000)

    def _encode_request(self, request):
        """Encode a request object"""
        obj = request_to_dict(request, self.spider)
        return self.serializer.dumps(obj)

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        obj = self.serializer.loads(encoded_request)
        return request_from_dict(obj, self.spider)

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop a request"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        self.server.delete(self.key)


class FifoQueue(Base):
    """Per-spider FIFO queue"""

    def __len__(self):
        """Return the length of the queue"""
        return self.server.llen(self.key)

    def push(self, request):
        """Push a request"""
        fp = request_fingerprint(request)
        bucket = str(self.bucket.mapping(fp))
        bucket_key = self.key + "_" + bucket
        self.server.lpush(bucket_key, self._encode_request(request))
        logger.debug("[lpush] key is {}, fp is {}".format(bucket_key, fp))
        # 某order requests下存在的桶（hash计数）
        self.server.hincrby(self.exist_bucket_key, bucket, 1)
        logger.debug("[hincrby-bucket] exsit buket key is {}, bucket is {}".format(self.exist_bucket_key, bucket))

    def pop(self, timeout=0):
        """Pop a request"""
        data = None
        bucket = None
        bucket_count_map = self.server.hgetall(self.exist_bucket_key)
        # 遍历dict
        if bucket_count_map:
            for entry in bucket_count_map:
                if int(bucket_count_map[entry].decode('utf-8')) > 0:
                    # 拿到有效桶号
                    bucket = entry
                    logger.debug("[hgetall-bucket] bucket key is {}, bucket is {}".format(self.exist_bucket_key, bucket))
                    break
        if timeout > 0:
            # 一秒钟去看看有没有数据
            while timeout > 0:

                if bucket:
                    data = self.server.rpop(self.key + "_" + bucket.decode("utf-8"))
                    if data:
                        logger.debug("[rpop] key is {}, data is {}".format(self.key + "_" + bucket.decode("utf-8"),
                                                                           self._decode_request(data)))
                        self.server.hincrby(self.exist_bucket_key, bucket, -1)
                        if isinstance(data, tuple):
                            data = data[1]
                        break
                time.sleep(1)
                timeout = timeout - 1
        else:
            data = self.server.rpop(self.key + "_" + bucket.decode("utf-8"))
        if data:
            return self._decode_request(data)


class PriorityQueue(Base):
    """Per-spider priority queue abstraction using redis' sorted set"""

    def __len__(self):
        """Return the length of the queue"""
        return self.server.zcard(self.key)

    def push(self, request):
        """Push a request"""
        data = self._encode_request(request)
        score = -request.priority
        # We don't use zadd method as the order of arguments change depending on
        # whether the class is Redis or StrictRedis, and the option of using
        # kwargs only accepts strings, not bytes.
        self.server.execute_command('ZADD', self.key, score, data)

    def pop(self, timeout=0):
        """
        Pop a request
        timeout not support in this queue class
        """
        # use atomic range/remove using multi/exec
        pipe = self.server.pipeline()
        pipe.multi()
        pipe.zrange(self.key, 0, 0).zremrangebyrank(self.key, 0, 0)
        results, count = pipe.execute()
        if results:
            return self._decode_request(results[0])


class LifoQueue(Base):
    """Per-spider LIFO queue."""

    def __len__(self):
        """Return the length of the stack"""
        return self.server.llen(self.key)

    def push(self, request):
        """Push a request"""
        self.server.lpush(self.key, self._encode_request(request))

    def pop(self, timeout=0):
        """Pop a request"""
        if timeout > 0:
            # 一秒钟去看看有没有数据
            while timeout > 0:
                data = self.server.lpop(self.key)
                if data:
                    break
                time.sleep(1)
                timeout = timeout - 1

            if isinstance(data, tuple):
                data = data[1]
        else:
            data = self.server.lpop(self.key)

        if data:
            return self._decode_request(data)


# TODO: Deprecate the use of these names.
SpiderQueue = FifoQueue
SpiderStack = LifoQueue
SpiderPriorityQueue = PriorityQueue
