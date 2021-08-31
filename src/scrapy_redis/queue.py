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
        self.bucket_size = spider.settings.getint('SCHEDULER_QUEUE_BUCKET_SIZE')
        self.bucket = BucketHash(self.bucket_size)

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
        fp = request_fingerprint(request)
        logger.debug("[push] fp is {} start".format(fp))
        bucket = str(self.bucket.mapping(fp))
        bucket_key = self.key + "_" + bucket

        data = self._encode_request(request)
        score = -request.priority
        # We don't use zadd method as the order of arguments change depending on
        # whether the class is Redis or StrictRedis, and the option of using
        # kwargs only accepts strings, not bytes.
        if score == 0:
            logger.info("[push] request url is {}".format(request.url))
            prio_bucket = self.bucket_size + 1
            bucket_key = self.key + "_" + str(prio_bucket)
        logging.debug("[zadd] buket_key is {} start...".format(bucket_key))
        self.server.execute_command('ZADD', bucket_key, score, data)
        logging.debug("[zadd] buket_key is {} end...".format(bucket_key))
        self.server.hincrby(self.exist_bucket_key, bucket, 1)
        logger.debug("[hincrby-bucket] exsit buket key is {}, bucket is {}".format(self.exist_bucket_key, bucket))
        logger.debug("[push] fp is {} end".format(fp))

    def pop(self, timeout=0):
        """
        Pop a request
        timeout not support in this queue class
        """
        logger.debug("[pop] key is {} start".format(self.key))
        # 先读优先级桶, 桶号为bucket_size + 1, 读最高优先级的桶，如果有直接返回
        prio_bucket = self.bucket_size + 1
        bucket_key = self.key + "_" + str(prio_bucket)
        logger.debug("[pop] bucket_key is {} start".format(bucket_key))
        results = self.server.zrange(bucket_key, 0, 0)
        count = self.server.zremrangebyrank(bucket_key, 0, 0)
        logger.debug("[pop] bucket_key is {} end".format(bucket_key))
        if results:
            return self._decode_request(results[0])
        bucket = None
        bucket_count_map = self.server.hgetall(self.exist_bucket_key)
        if bucket_count_map:
            for entry in bucket_count_map:
                if int(bucket_count_map[entry].decode('utf-8')) > 0:
                    # 拿到有效桶号
                    bucket = entry
                    logger.debug("[hgetall-bucket] bucket key is {}, bucket is {}".format(self.exist_bucket_key, bucket))
                    break
        if not bucket:
            return None

        bucket_key = self.key + "_" + bucket.decode("utf-8")
        logger.debug("[pop] bucket_key is {} start".format(bucket_key))
        results = self.server.zrange(bucket_key, 0, 0)
        count = self.server.zremrangebyrank(bucket_key, 0, 0)
        logger.debug("[pop] bucket_key is {} end".format(bucket_key))
        self.server.hincrby(self.exist_bucket_key, bucket, -1)
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
