#-*- coding:utf-8 -*-
# Copyright 2019 jiangshu, Inc.
# 2021-07-23 16:48
__author__ = 'yichao.wu'

import hashlib


class BucketHash:

    def __init__(self, size):
        """Initialize the duplicates filter.

        Parameters
        ----------
        size : 桶的大小

        """
        self.size = size

    def md5(self, key):
        md5 = hashlib.md5()
        md5.update(key.encode('utf-8'))
        return md5.hexdigest()

    def mapping(self, key):
        hash_key = self.md5(key)
        # 取前8个字节，也就是长度为16的字符串，作为hexString
        hash_key_part = hash_key[0:16]
        number_bytes = bytes.fromhex(hash_key_part)
        num_64 = int.from_bytes(number_bytes, byteorder='big')
        return num_64 % self.size

