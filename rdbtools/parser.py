# This Python file uses the following encoding: utf-8
import struct
import io
import sys
import datetime
import re

try :
    from StringIO import StringIO
except ImportError:
    from io import StringIO

REDIS_RDB_6BITLEN = 0
REDIS_RDB_14BITLEN = 1
REDIS_RDB_32BITLEN = 2
REDIS_RDB_ENCVAL = 3

REDIS_RDB_OPCODE_EXPIRETIME_MS = 252 #是否有过期设置
REDIS_RDB_OPCODE_EXPIRETIME = 253    #是否有过期设置
REDIS_RDB_OPCODE_SELECTDB = 254      #数据库前缀
REDIS_RDB_OPCODE_EOF = 255           #rdb文件结束

#redis data type
REDIS_RDB_TYPE_STRING = 0
REDIS_RDB_TYPE_LIST = 1
REDIS_RDB_TYPE_SET = 2
REDIS_RDB_TYPE_ZSET = 3
REDIS_RDB_TYPE_HASH = 4
REDIS_RDB_TYPE_HASH_ZIPMAP = 9
REDIS_RDB_TYPE_LIST_ZIPLIST = 10
REDIS_RDB_TYPE_SET_INTSET = 11
REDIS_RDB_TYPE_ZSET_ZIPLIST = 12
REDIS_RDB_TYPE_HASH_ZIPLIST = 13

REDIS_RDB_ENC_INT8 = 0
REDIS_RDB_ENC_INT16 = 1
REDIS_RDB_ENC_INT32 = 2
REDIS_RDB_ENC_LZF = 3

DATA_TYPE_MAPPING = {
    0 : "string", 1 : "list", 2 : "set", 3 : "sortedset", 4 : "hash",
    9 : "hash", 10 : "list", 11 : "set", 12 : "sortedset", 13 : "hash"}

class RdbCallback:
    """
    A Callback to handle events as the Redis dump file is parsed.
    This callback provides a serial and fast access to the dump file.

    """
    def start_rdb(self):
        """
        Called once we know we are dealing with a valid redis dump file

        """
        pass

    def start_database(self, db_number):
        """
        Called to indicate database the start of database `db_number`

        Once a database starts, another database cannot start unless
        the first one completes and then `end_database` method is called

        Typically, callbacks store the current database number in a class variable

        """
        pass

    def set(self, key, value, expiry, info):
        """
        Callback to handle a key with a string value and an optional expiry

        `key` is the redis key
        `value` is a string or a number
        `expiry` is a datetime object. None and can be None
        `info` is a dictionary containing additional information about this object.

        """
        pass

    def start_hash(self, key, length, expiry, info):
        """Callback to handle the start of a hash

        `key` is the redis key
        `length` is the number of elements in this hash.
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.

        After `start_hash`, the method `hset` will be called with this `key` exactly `length` times.
        After that, the `end_hash` method will be called.

        """
        pass

    def hset(self, key, field, value):
        """
        Callback to insert a field=value pair in an existing hash

        `key` is the redis key for this hash
        `field` is a string
        `value` is the value to store for this field

        """
        pass

    def end_hash(self, key):
        """
        Called when there are no more elements in the hash

        `key` is the redis key for the hash

        """
        pass

    def start_set(self, key, cardinality, expiry, info):
        """
        Callback to handle the start of a hash

        `key` is the redis key
        `cardinality` is the number of elements in this set
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.

        After `start_set`, the  method `sadd` will be called with `key` exactly `cardinality` times
        After that, the `end_set` method will be called to indicate the end of the set.

        Note : This callback handles both Int Sets and Regular Sets

        """
        pass

    def sadd(self, key, member):
        """
        Callback to inser a new member to this set

        `key` is the redis key for this set
        `member` is the member to insert into this set

        """
        pass

    def end_set(self, key):
        """
        Called when there are no more elements in this set

        `key` the redis key for this set

        """
        pass

    def start_list(self, key, length, expiry, info):
        """
        Callback to handle the start of a list

        `key` is the redis key for this list
        `length` is the number of elements in this list
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.

        After `start_list`, the method `rpush` will be called with `key` exactly `length` times
        After that, the `end_list` method will be called to indicate the end of the list

        Note : This callback handles both Zip Lists and Linked Lists.

        """
        pass

    def rpush(self, key, value) :
        """
        Callback to insert a new value into this list

        `key` is the redis key for this list
        `value` is the value to be inserted

        Elements must be inserted to the end (i.e. tail) of the existing list.

        """
        pass

    def end_list(self, key):
        """
        Called when there are no more elements in this list

        `key` the redis key for this list

        """
        pass

    def start_sorted_set(self, key, length, expiry, info):
        """
        Callback to handle the start of a sorted set

        `key` is the redis key for this sorted
        `length` is the number of elements in this sorted set
        `expiry` is a `datetime` object. None means the object does not expire
        `info` is a dictionary containing additional information about this object.

        After `start_sorted_set`, the method `zadd` will be called with `key` exactly `length` times.
        Also, `zadd` will be called in a sorted order, so as to preserve the ordering of this sorted set.
        After that, the `end_sorted_set` method will be called to indicate the end of this sorted set

        Note : This callback handles sorted sets in that are stored as ziplists or skiplists

        """
        pass

    def zadd(self, key, score, member):
        """Callback to insert a new value into this sorted set

        `key` is the redis key for this sorted set
        `score` is the score for this `value`
        `value` is the element being inserted
        """
        pass

    def end_sorted_set(self, key):
        """
        Called when there are no more elements in this sorted set

        `key` is the redis key for this sorted set

        """
        pass

    def end_database(self, db_number):
        """
        Called when the current database ends

        After `end_database`, one of the methods are called -
        1) `start_database` with a new database number
            OR
        2) `end_rdb` to indicate we have reached the end of the file

        """
        pass

    def end_rdb(self):
        """Called to indicate we have completed parsing of the dump file"""
        pass

class RdbParser :
    """
    A Parser for Redis RDB Files

    This class is similar in spirit to a SAX parser for XML files.
    The dump file is parsed sequentially. As and when objects are discovered,
    appropriate methods in the callback are called.

    Typical usage :
        callback = MyRdbCallback() # Typically a subclass of RdbCallback
        parser = RdbParser(callback)
        parser.parse('/var/redis/6379/dump.rdb')

    filter is a dictionary with the following keys
        {"dbs" : [0, 1], "keys" : "foo.*", "types" : ["hash", "set", "sortedset", "list", "string"]}

        If filter is None, results will not be filtered
        If dbs, keys or types is None or Empty, no filtering will be done on that axis
    """
    def __init__(self, callback, filters = None) :
        """
            `callback` is the object that will receive parse events
        """
        self._callback = callback
        self._key = None
        self._expiry = None
        self.init_filter(filters)
    
    # +-------+-------------+-----------+-----------------+-----+-----------+
    # | REDIS | RDB-VERSION | SELECT-DB | KEY-VALUE-PAIRS | EOF | CHECK-SUM |
    # +-------+-------------+-----------+-----------------+-----+-----------+
    # 
    #                       |<-------- DB-DATA ---------->|
    #
    # RDB-VERSION : 不同版本的 RDB 文件互不兼容，所以在读入程序时，需要根据版本来选择不同的读入方式
    # DB-DATA : 一个 RDB 文件中会出现任意多次DB-DATA，每个 DB-DATA 部分保存着服务器上一个非空数据库的所有数据
    # SELECT-DB : 这域保存着跟在后面的键值对所属的数据库号码, 在读入 RDB 文件时，程序会根据这个域的值来切换数据库，确保数据被还原到正确的数据库上
    # KEY-VALUE-PAIRS : 每个键值对的数据使用以下结构来保存
    #    +----------------------+---------------+-----+-------+
    #    | OPTIONAL-EXPIRE-TIME | TYPE-OF-VALUE | KEY | VALUE |
    #    +----------------------+---------------+-----+-------+
    #     
    #    OPTIONAL-EXPIRE-TIME: 是可选的，如果键没有设置过期时间，那么这个域就不会出现； 反之，如果这个域出现的话，那么它记录着键的过期时间，在当前版本的 RDB 中，过期时间是一个以毫秒为单位的 UNIX 时间戳
    #    KEY: 域保存着键，格式和 REDIS_ENCODING_RAW 编码的字符串对象一样
    #    TYPE-OF-VALUE: 记录着 VALUE 域的值所使用的编码， 根据这个域的指示， 程序会使用不同的方式来保存和读取 VALUE 的值
    #    VALUE: 
    #        REDIS_ENCODING_INT 编码的 REDIS_STRING 类型对象:
    #            如果值可以表示为 8 位、 16 位或 32 位有符号整数，那么直接以整数类型的形式来保存它们：
    #                +---------+
    #                | integer |
    #                +---------+
    #            如果值不能被表示为最高 32 位的有符号整数，那么说明这是一个 long 类型的值，在 RDB 文件中，这种类型的值以字符序列的形式保存
    #            一个字符序列由两部分组成：
    #                +-----+---------+
    #                | LEN | CONTENT |
    #                +-----+---------+
    #                其中， CONTENT 域保存了字符内容，而 LEN 则保存了以字节为单位的字符长度
    #                当进行载入时，读入器先读入 LEN ，创建一个长度等于 LEN 的字符串对象，然后再从文件中读取 LEN 字节数据，并将这些数据设置为字符串对象的值
    #
    #        REDIS_ENCODING_RAW 编码的 REDIS_STRING 类型值有三种保存方式：
    #            1. 如果值可以表示为 8 位、 16 位或 32 位长的有符号整数，那么用整数类型的形式来保存它们
    #            2. 如果字符串长度大于 20 ，并且服务器开启了 LZF 压缩功能 ，那么对字符串进行压缩，并保存压缩之后的数据。
    #            经过 LZF 压缩的字符串会被保存为以下结构：
    #                +----------+----------------+--------------------+
    #                | LZF-FLAG | COMPRESSED-LEN | COMPRESSED-CONTENT |
    #                +----------+----------------+--------------------+
    #                LZF-FLAG 告知读入器，后面跟着的是被 LZF 算法压缩过的数据
    #                COMPRESSED-CONTENT 是被压缩后的数据， COMPRESSED-LEN 则是该数据的字节长度
    #            3. 在其他情况下，程序直接以普通字节序列的方式来保存字符串。比如说，对于一个长度为 20 字节的字符串，需要使用 20 字节的空间来保存它
    #            这种字符串被保存为以下结构:
    #                +-----+---------+
    #                | LEN | CONTENT |
    #                +-----+---------+
    #                LEN 为字符串的字节长度， CONTENT 为字符串
    #        当进行载入时，读入器先检测字符串保存的方式，再根据不同的保存方式，用不同的方法取出内容，并将内容保存到新建的字符串对象当中
    #
    #        REDIS_ENCODING_LINKEDLIST 编码的 REDIS_LIST 类型值保存为以下结构:
    #            +-----------+--------------+--------------+-----+--------------+
    #            | NODE-SIZE | NODE-VALUE-1 | NODE-VALUE-2 | ... | NODE-VALUE-N |
    #            +-----------+--------------+--------------+-----+--------------+
    #            其中 NODE-SIZE 保存链表节点数量，后面跟着任意多个节点值。节点值的保存方式和字符串的保存方式一样
    #            当进行载入时，读入器读取节点的数量，创建一个新的链表，然后一直执行以下步骤，直到指定节点数量满足为止:
    #                1. 读取字符串表示的节点值
    #                2. 将包含节点值的新节点添加到链表中
    #
    #        REDIS_ENCODING_HT 编码的 REDIS_SET 类型值保存为以下结构:
    #            +----------+-----------+-----------+-----+-----------+
    #            | SET-SIZE | ELEMENT-1 | ELEMENT-2 | ... | ELEMENT-N |
    #            +----------+-----------+-----------+-----+-----------+
    #            SET-SIZE 记录了集合元素的数量，后面跟着多个元素值。元素值的保存方式和字符串的保存方式一样
    #            载入时，读入器先读入集合元素的数量 SET-SIZE ，再连续读入 SET-SIZE 个字符串，并将这些字符串作为新元素添加至新创建的集合
    #
    #        REDIS_ENCODING_SKIPLIST 编码的 REDIS_ZSET 类型值保存为以下结构：
    #            +--------------+-------+---------+-------+---------+-----+-------+---------+
    #            | ELEMENT-SIZE | MEB-1 | SCORE-1 | MEB-2 | SCORE-2 | ... | MEB-N | SCORE-N |
    #            +--------------+-------+---------+-------+---------+-----+-------+---------+
    #            其中 ELEMENT-SIZE 为有序集元素的数量， MEB-i 为第 i 个有序集元素的成员， SCORE-i 为第 i 个有序集元素的分值
    #            当进行载入时，读入器读取有序集元素数量，创建一个新的有序集，然后一直执行以下步骤，直到指定元素数量满足为止：
    #                1. 读入字符串形式保存的成员 member
    #                2. 读入字符串形式保存的分值 score ，并将它转换为浮点数
    #                3. 添加 member 为成员、 score 为分值的新元素到有序集
    #
    #        REDIS_ENCODING_HT 编码的 REDIS_HASH 类型值保存为以下结构：
    #            +-----------+-------+---------+-------+---------+-----+-------+---------+
    #            | HASH-SIZE | KEY-1 | VALUE-1 | KEY-2 | VALUE-2 | ... | KEY-N | VALUE-N |
    #            +-----------+-------+---------+-------+---------+-----+-------+---------+
    #            HASH-SIZE 是哈希表包含的键值对的数量， KEY-i 和 VALUE-i 分别是哈希表的键和值
    #            载入时，程序先创建一个新的哈希表，然后读入 HASH-SIZE ，再执行以下步骤 HASH-SIZE 次：
    #                1. 读入一个字符串
    #                2. 再读入另一个字符串
    #                3. 将第一个读入的字符串作为键，第二个读入的字符串作为值，插入到新建立的哈希中
    #
    #        REDIS_LIST 类型、 REDIS_HASH 类型和 REDIS_ZSET 类型都使用了 REDIS_ENCODING_ZIPLIST 编码， ziplist 在 RDB 中的保存方式如下：
    #            +-----+---------+
    #            | LEN | ZIPLIST |
    #            +-----+---------+
    #            载入时，读入器先读入 ziplist 的字节长，再根据该字节长读入数据，最后将数据还原成一个 ziplist
    #
    #        REDIS_ENCODING_INTSET 编码的 REDIS_SET 类型值保存为以下结构：
    #            +-----+--------+
    #            | LEN | INTSET |
    #            +-----+--------+
    #            载入时，读入器先读入 intset 的字节长度，再根据长度读入数据，最后将数据还原成 intset
    #
    #    EOF: 标志着数据库内容的结尾（不是文件的结尾），值为 rdb.h/EDIS_RDB_OPCODE_EOF （255）
    #
    #    CHECK-SUM: 
    #        RDB 文件所有内容的校验和， 一个 uint_64t 类型值
    #        REDIS 在写入 RDB 文件时将校验和保存在 RDB 文件的末尾， 当读取时， 根据它的值对内容进行校验
    #        如果这个域的值为 0 ， 那么表示 Redis 关闭了校验和功能
    #    
    #    以上注释都来自于http://www.redisbook.com/en/latest/internal/rdb.html 如果大家看完觉得有收获还是支持下作者，捐赠点儿，鼓励下作者。
    def parse(self, filename):
        """
        Parse a redis rdb dump file, and call methods in the
        callback object during the parsing operation.
        """
        with open(filename, "rb") as f:
            self.verify_magic_string(f.read(5))
            self.verify_version(f.read(4))
            self._callback.start_rdb()

            is_first_database = True
            db_number = 0
            while True :
                self._expiry = None
                data_type = read_unsigned_char(f)

                if data_type == REDIS_RDB_OPCODE_EXPIRETIME_MS :
                    self._expiry = to_datetime(read_unsigned_long(f) * 1000)
                    data_type = read_unsigned_char(f)
                elif data_type == REDIS_RDB_OPCODE_EXPIRETIME :
                    self._expiry = to_datetime(read_unsigned_int(f) * 1000000)
                    data_type = read_unsigned_char(f)

                if data_type == REDIS_RDB_OPCODE_SELECTDB :
                    if not is_first_database :
                        self._callback.end_database(db_number)
                    is_first_database = False
                    db_number = self.read_length(f)
                    self._callback.start_database(db_number)
                    continue

                if data_type == REDIS_RDB_OPCODE_EOF :
                    self._callback.end_database(db_number)
                    self._callback.end_rdb()
                    break

                if self.matches_filter(db_number) :
                    self._key = self.read_string(f)
                    if self.matches_filter(db_number, self._key, data_type):
                        self.read_object(f, data_type)
                    else:
                        self.skip_object(f, data_type)
                else :
                    self.skip_key_and_object(f, data_type)

    def read_length_with_encoding(self, f) :
        length = 0
        is_encoded = False
        bytes = []
        bytes.append(read_unsigned_char(f))
        enc_type = (bytes[0] & 0xC0) >> 6
        if enc_type == REDIS_RDB_ENCVAL :#REDIS_RDB_ENCVAL 3 如果字符串是通过编码后存储的，则存储长度的类型的位表示为11，然后根据后6位的编码类型来确定怎样读取和解析接下来的数据
            is_encoded = True
            length = bytes[0] & 0x3F
        elif enc_type == REDIS_RDB_6BITLEN : #REDIS_RDB_6BITLEN 0 剩余的6位保存长度
            length = bytes[0] & 0x3F
        elif enc_type == REDIS_RDB_14BITLEN : #REDIS_RDB_14BITLEN 1 剩余的14位保存长度
            bytes.append(read_unsigned_char(f))
            length = ((bytes[0]&0x3F)<<8)|bytes[1]
        else : #REDIS_RDB_32BITLEN 2 接下来的4个字节中保存长度
            length = ntohl(f)
        return (length, is_encoded)

    def read_length(self, f) :
        return self.read_length_with_encoding(f)[0]

    # 字符串类型分别使用 REDIS_ENCODING_INT 和 REDIS_ENCODING_RAW 两种编码：
    #     REDIS_ENCODING_INT 使用 long 类型来保存 long 类型值
    #     REDIS_ENCODING_RAW 则使用 sdshdr 结构来保存 sds （也即是 char* )、 long long 、 double 和 long double 类型值
    #
    #     REDIS_ENCODING_INT 编码的 REDIS_STRING 类型对象：
    #         如果值可以表示为 8 位、 16 位或 32 位有符号整数，那么直接以整数类型的形式来保存它们：
    #         +---------+
    #         | integer |
    #         +---------+
    #         比如说，整数 8 可以用 8 位序列 00001000 保存。
    #         当读入这类值时，程序按指定的长度读入字节数据，然后将数据转换回整数类型
    #
    #    REDIS_ENCODING_RAW 编码的 REDIS_STRING 类型值有三种保存方式：
    #        1. 如果值可以表示为 8 位、 16 位或 32 位长的有符号整数，那么用整数类型的形式来保存它们
    #        2. 如果字符串长度大于 20 ，并且服务器开启了 LZF 压缩功能 ，那么对字符串进行压缩，并保存压缩之后的数据
    #           经过 LZF 压缩的字符串会被保存为以下结构：
    #           +----------+----------------+--------------------+
    #           | LZF-FLAG | COMPRESSED-LEN | COMPRESSED-CONTENT |
    #           +----------+----------------+--------------------+
    #           LZF-FLAG 告知读入器，后面跟着的是被 LZF 算法压缩过的数据
    #           COMPRESSED-CONTENT 是被压缩后的数据， COMPRESSED-LEN 则是该数据的字节长度
    #        3. 在其他情况下，程序直接以普通字节序列的方式来保存字符串。比如说，对于一个长度为 20 字节的字符串，需要使用 20 字节的空间来保存它
    #           这种字符串被保存为以下结构：
    #           +-----+---------+
    #           | LEN | CONTENT |
    #           +-----+---------+
    #           LEN 为字符串的字节长度， CONTENT 为字符串
    # 当进行载入时，读入器先检测字符串保存的方式，再根据不同的保存方式，用不同的方法取出内容，并将内容保存到新建的字符串对象当中
    def read_string(self, f) :
        tup = self.read_length_with_encoding(f)
        length = tup[0]
        is_encoded = tup[1]
        val = None
        if is_encoded : #REDIS_RDB_ENCVAL 3 时, is_encoded为true
            if length == REDIS_RDB_ENC_INT8 : #REDIS_RDB_ENC_INT8 0  8 bit signed integer
                val = read_signed_char(f)
            elif length == REDIS_RDB_ENC_INT16 : #REDIS_RDB_ENC_INT16 1 16 bit signed integer
                val = read_signed_short(f)
            elif length == REDIS_RDB_ENC_INT32 : #REDIS_RDB_ENC_INT32 2 32 bit signed integer
                val = read_signed_int(f)
            elif length == REDIS_RDB_ENC_LZF : #REDIS_RDB_ENC_LZF 3 string compressed with FASTLZ
                clen = self.read_length(f)
                l = self.read_length(f)
                val = self.lzf_decompress(f.read(clen), l)
        else :
            val = f.read(length)
        return val

    # Read an object for the stream
    # f is the redis file
    # enc_type is the type of object
    #
    # REDIS_HASH_ZIPMAP，REDIS_LIST_ZIPLIST，REDIS_SET_INTSET和REDIS_ZSET_ZIPLIST这四种数据类型都是只在rdb文件中才有的类型，其他的数据类型其实就是val对象中type字段存储的值
    def read_object(self, f, enc_type) :
        if enc_type == REDIS_RDB_TYPE_STRING : # REDIS_RDB_TYPE_STRING = 0 字符串
            val = self.read_string(f)
            self._callback.set(self._key, val, self._expiry, info={'encoding':'string'})
        elif enc_type == REDIS_RDB_TYPE_LIST : # REDIS_RDB_TYPE_LIST = 1
            # A redis list is just a sequence of strings
            # We successively read strings from the stream and create a list from it
            # The lists are in order i.e. the first string is the head,
            # and the last string is the tail of the list
            length = self.read_length(f)
            self._callback.start_list(self._key, length, self._expiry, info={'encoding':'linkedlist' })
            for count in xrange(0, length) :
                val = self.read_string(f)
                self._callback.rpush(self._key, val)
            self._callback.end_list(self._key)
        elif enc_type == REDIS_RDB_TYPE_SET : # REDIS_RDB_TYPE_SET = 2 这里的set是无序的(non-deterministic)
            # A redis list is just a sequence of strings
            # We successively read strings from the stream and create a set from it
            # Note that the order of strings is non-deterministic
            length = self.read_length(f)
            self._callback.start_set(self._key, length, self._expiry, info={'encoding':'hashtable'})
            for count in xrange(0, length) :
                val = self.read_string(f)
                self._callback.sadd(self._key, val)
            self._callback.end_set(self._key)
        elif enc_type == REDIS_RDB_TYPE_ZSET : # REDIS_RDB_TYPE_ZSET = 3
            length = self.read_length(f)
            self._callback.start_sorted_set(self._key, length, self._expiry, info={'encoding':'skiplist'})
            for count in xrange(0, length) :
                val = self.read_string(f)
                dbl_length = read_unsigned_char(f)
                score = f.read(dbl_length)
                if isinstance(score, str):
                    score = float(score)
                self._callback.zadd(self._key, score, val)
            self._callback.end_sorted_set(self._key)

        # +-----------+------+---------+-------+--------+----------+
        # | entry-num | key1 | value1  |  ...  |  keyn  |  valuen  |
        # +-----------+------+---------+-------+--------+----------+ 
        # entry-num : hash中[键值对]的数量
        # key : string类型的key
        # value : string类型的value
        elif enc_type == REDIS_RDB_TYPE_HASH : # REDIS_RDB_TYPE_HASH = 4
            length = self.read_length(f)
            self._callback.start_hash(self._key, length, self._expiry, info={'encoding':'hashtable'})
            for count in xrange(0, length) :
                field = self.read_string(f) # read key
                value = self.read_string(f) # read value
                self._callback.hset(self._key, field, value)
            self._callback.end_hash(self._key)
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPMAP : # REDIS_RDB_TYPE_HASH_ZIPMAP = 9
            self.read_zipmap(f)
        elif enc_type == REDIS_RDB_TYPE_LIST_ZIPLIST : # REDIS_RDB_TYPE_LIST_ZIPLIST = 10
            self.read_ziplist(f)
        elif enc_type == REDIS_RDB_TYPE_SET_INTSET : # REDIS_RDB_TYPE_SET_INTSET = 11
            self.read_intset(f)
        elif enc_type == REDIS_RDB_TYPE_ZSET_ZIPLIST : # REDIS_RDB_TYPE_ZSET_ZIPLIST = 12
            self.read_zset_from_ziplist(f)
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPLIST : # REDIS_RDB_TYPE_HASH_ZIPLIST = 13
            self.read_hash_from_ziplist(f)
        else :
            raise Exception('read_object', 'Invalid object type %d for key %s' % (enc_type, self._key))

    def skip_key_and_object(self, f, data_type):
        self.skip_string(f)
        self.skip_object(f, data_type)

    def skip_string(self, f):
        tup = self.read_length_with_encoding(f)
        length = tup[0]
        is_encoded = tup[1]
        bytes_to_skip = 0
        if is_encoded :
            if length == REDIS_RDB_ENC_INT8 :
                bytes_to_skip = 1
            elif length == REDIS_RDB_ENC_INT16 :
                bytes_to_skip = 2
            elif length == REDIS_RDB_ENC_INT32 :
                bytes_to_skip = 4
            elif length == REDIS_RDB_ENC_LZF :
                clen = self.read_length(f)
                l = self.read_length(f)
                bytes_to_skip = clen
        else :
            bytes_to_skip = length

        skip(f, bytes_to_skip)

    def skip_object(self, f, enc_type):
        skip_strings = 0
        if enc_type == REDIS_RDB_TYPE_STRING :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_LIST :
            skip_strings = self.read_length(f)
        elif enc_type == REDIS_RDB_TYPE_SET :
            skip_strings = self.read_length(f)
        elif enc_type == REDIS_RDB_TYPE_ZSET :
            skip_strings = self.read_length(f) * 2
        elif enc_type == REDIS_RDB_TYPE_HASH :
            skip_strings = self.read_length(f) * 2
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPMAP :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_LIST_ZIPLIST :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_SET_INTSET :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_ZSET_ZIPLIST :
            skip_strings = 1
        elif enc_type == REDIS_RDB_TYPE_HASH_ZIPLIST :
            skip_strings = 1
        else :
            raise Exception('read_object', 'Invalid object type %d for key %s' % (enc_type, self._key))
        for x in xrange(0, skip_strings):
            self.skip_string(f)

    
    # +-----+--------+
    # | LEN | INTSET |
    # +-----+--------+ 
    #
    # intset数据格式
    # typedef struct intset {
    #     // 保存元素所使用的类型的长度
    #     uint32_t encoding;
    #     // 元素个数
    #     uint32_t length;
    #     // 保存元素的数组
    #     int8_t contents[];
    # } intset;
    #
    # TODO : raw_string这个是怎么回事儿,一起读出来encoding和length,为什么要一起读出来,怎么一起读出来的？
    # FIXED : 通过LEN读取整个INTSET放入raw_string中，注意整数集的总体结构：
    # +-----+--------+
    # | LEN | INTSET |
    # +-----+--------+
    def read_intset(self, f) :
        raw_string = self.read_string(f)
        buff = StringIO(raw_string)
        encoding = read_unsigned_int(buff)
        num_entries = read_unsigned_int(buff)
        self._callback.start_set(self._key, num_entries, self._expiry, info={'encoding':'intset', 'sizeof_value':len(raw_string)})
        for x in xrange(0, num_entries) :
            if encoding == 8 :
                entry = read_unsigned_long(buff)
            elif encoding == 4 :
                entry = read_unsigned_int(buff)
            elif encoding == 2 :
                entry = read_unsigned_short(buff)
            else :
                raise Exception('read_intset', 'Invalid encoding %d for key %s' % (encoding, self._key))
            self._callback.sadd(self._key, entry)
        self._callback.end_set(self._key)
    
    # area        |<---- ziplist header ---->|<----------- entries ------------->|<-end->|
    # 
    # size          4 bytes  4 bytes  2 bytes    ?        ?        ?        ?     1 byte
    #             +---------+--------+-------+--------+--------+--------+--------+-------+
    # component   | zlbytes | zltail | zllen | entry1 | entry2 |  ...   | entryN | zlend |
    #             +---------+--------+-------+--------+--------+--------+--------+-------+
    #                                        ^                          ^        ^
    # address                                |                          |        |
    #                                 ZIPLIST_ENTRY_HEAD                |   ZIPLIST_ENTRY_END
    #                                                                   |
    #                                                          ZIPLIST_ENTRY_TAIL
    #
    # zlbytes   uint32_t    整个 ziplist 占用的内存字节数，对 ziplist 进行内存重分配，或者计算末端时使用
    # zltail    uint32_t    到达 ziplist 表尾节点的偏移量。 通过这个偏移量，可以在不遍历整个 ziplist 的前提下，弹出表尾节点
    # zllen     uint16_t    ziplist 中节点的数量。 当这个值小于 UINT16_MAX （65535）时，这个值就是 ziplist 中节点的数量； 当这个值等于 UINT16_MAX 时，节点的数量需要遍历整个 ziplist 才能计算得出
    # entryX    ?           ziplist 所保存的节点，各个节点的长度根据内容而定。
    # zlend     uint8_t     255 的二进制值 1111 1111 （UINT8_MAX） ，用于标记 ziplist 的末端
    def read_ziplist(self, f) :
        raw_string = self.read_string(f)
        buff = StringIO(raw_string)
        zlbytes = read_unsigned_int(buff)
        tail_offset = read_unsigned_int(buff)
        num_entries = read_unsigned_short(buff)
        self._callback.start_list(self._key, num_entries, self._expiry, info={'encoding':'ziplist', 'sizeof_value':len(raw_string)})
        for x in xrange(0, num_entries) :
            val = self.read_ziplist_entry(buff)
            self._callback.rpush(self._key, val)
        zlist_end = read_unsigned_char(buff)
        if zlist_end != 255 :
            raise Exception('read_ziplist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_list(self._key)

    #           |<--  element 1 -->|<--  element 2 -->|<--   .......   -->|
    # 
    # +---------+---------+--------+---------+--------+---------+---------+---------+
    # | ZIPLIST |         |        |         |        |         |         | ZIPLIST |
    # | ENTRY   | member1 | score1 | member2 | score2 |   ...   |   ...   | ENTRY   |
    # | HEAD    |         |        |         |        |         |         | END     |
    # +---------+---------+--------+---------+--------+---------+---------+---------+
    # 
    # score1 <= score2 <= ...
    #
    # 当使用 REDIS_ENCODING_ZIPLIST 编码时， 有序集将元素保存到 ziplist 数据结构里面
    # 其中，每个有序集元素以两个相邻的 ziplist 节点表示， 第一个节点保存元素的 member 域， 第二个元素保存元素的 score 域
    # 多个元素之间按 score 值从小到大排序， 如果两个元素的 score 相同， 那么按字典序对 member 进行对比， 决定那个元素排在前面， 那个元素排在后面
    def read_zset_from_ziplist(self, f) :
        raw_string = self.read_string(f)
        buff = StringIO(raw_string)
        zlbytes = read_unsigned_int(buff)
        tail_offset = read_unsigned_int(buff)
        num_entries = read_unsigned_short(buff)
        if (num_entries % 2) :
            raise Exception('read_zset_from_ziplist', "Expected even number of elements, but found %d for key %s" % (num_entries, self._key))
        num_entries = num_entries /2
        self._callback.start_sorted_set(self._key, num_entries, self._expiry, info={'encoding':'ziplist', 'sizeof_value':len(raw_string)})
        for x in xrange(0, num_entries) :
            member = self.read_ziplist_entry(buff)
            score = self.read_ziplist_entry(buff)
            if isinstance(score, str) :
                score = float(score)
            self._callback.zadd(self._key, score, member)
        zlist_end = read_unsigned_char(buff)
        if zlist_end != 255 :
            raise Exception('read_zset_from_ziplist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_sorted_set(self._key)
    
    # hashmap的键值对是作为连续的条目存储在ziplist里
    # 注意：这是在rdb版本4引入，它废弃了在先前版本里使用的zipmap
    def read_hash_from_ziplist(self, f) :
        raw_string = self.read_string(f)
        buff = StringIO(raw_string)
        zlbytes = read_unsigned_int(buff)
        tail_offset = read_unsigned_int(buff)
        num_entries = read_unsigned_short(buff)
        if (num_entries % 2) :
            raise Exception('read_hash_from_ziplist', "Expected even number of elements, but found %d for key %s" % (num_entries, self._key))
        num_entries = num_entries / 2
        self._callback.start_hash(self._key, num_entries, self._expiry, info={'encoding':'ziplist', 'sizeof_value':len(raw_string)})
        for x in xrange(0, num_entries) :
            field = self.read_ziplist_entry(buff)
            value = self.read_ziplist_entry(buff)
            self._callback.hset(self._key, field, value)
        zlist_end = read_unsigned_char(buff)
        if zlist_end != 255 :
            raise Exception('read_hash_from_ziplist', "Invalid zip list end - %d for key %s" % (zlist_end, self._key))
        self._callback.end_hash(self._key)

    # area        |<------------------- entry -------------------->|
    # 
    #             +------------------+----------+--------+---------+
    # component   | pre_entry_length | encoding | length | content |
    #             +------------------+----------+--------+---------+
    #
    # pre_entry_length 记录了前一个节点的长度，通过这个值，可以进行指针计算，从而跳转到上一个节点
    #
    # area        |<---- previous entry --->|<--------------- current entry ---------------->|
    # 
    # size          5 bytes                   1 byte             ?          ?        ?
    #             +-------------------------+-----------------------------+--------+---------+
    # component   | ...                     | pre_entry_length | encoding | length | content |
    #             |                         |                  |          |        |         |
    # value       |                         | 0000 0101        |    ?     |   ?    |    ?    |
    #             +-------------------------+-----------------------------+--------+---------+
    #             ^                         ^
    # address     |                         |
    #             p = e - 5                 e
    # 上图展示了如何通过一个节点向前跳转到另一个节点： 用指向当前节点的指针 e ， 减去 pre_entry_length 的值（0000 0101 的十进制值， 5）， 得出的结果就是指向前一个节点的地址 p
    # 根据编码方式的不同， pre_entry_length 域可能占用 1 字节或者 5 字节:
    #     1 字节：如果前一节点的长度小于 254 字节，便使用一个字节保存它的值
    #     5 字节：如果前一节点的长度大于等于 254 字节，那么将第 1 个字节的值设为 254 ，然后用接下来的 4 个字节保存实际长度
    #
    # 作为例子， 以下是个长度为 1 字节的 pre_entry_length 域， 域的值为 128 （二进制为 1000 0000 ）：
    # area        |<------------------- entry -------------------->|
    # 
    # size          1 byte             ?          ?        ?
    #             +------------------+----------+--------+---------+
    # component   | pre_entry_length | encoding | length | content |
    #             |                  |          |        |         |
    # value       | 1000 0000        |          |        |         |
    #             +------------------+----------+--------+---------+    
    # 而以下则是个长度为 5 字节的 pre_entry_length 域， 域的第一个字节被设为 254 的二进制 1111 1110 ， 而之后的四个字节则被设置为 10086 的二进制 10 0111 0110 0110 （多余的高位用 0 补完）：
    # area        |<------------------------------ entry ---------------------------------->|
    # 
    # size          5 bytes                                     ?          ?        ?
    #             +-------------------------------------------+----------+--------+---------+
    # component   | pre_entry_length                          | encoding | length | content |
    #             |                                           |          |        |         |
    #             | 11111110 00000000000000000010011101100110 | ?        | ?      | ?       |
    #             +-------------------------------------------+----------+--------+---------+
    #             |<------->|<------------------------------->|
    #               1 byte       4 bytes
    #
    # encoding 和 length 两部分一起决定了 content 部分所保存的数据的类型（以及长度）
    # 其中， encoding 域的长度为两个 bit ， 它的值可以是 00 、 01 、 10 和 11 ：
    #     00 、 01 和 10 表示 content 部分保存着字符数组
    #     11 表示 content 部分保存着整数
    #
    # 以 00 、 01 和 10 开头的字符数组的编码方式如下：
    #     00bbbbbb                                      1 byte  长度小于等于 63 字节的字符数组
    #     01bbbbbb xxxxxxxx                             2 byte  长度小于等于 16383 字节的字符数组
    #     10____ aaaaaaaa bbbbbbbb cccccccc dddddddd    5 byte  长度小于等于 4294967295 的字符数组
    # 表格中的下划线 _ 表示留空，而变量 b 、x 等则代表实际的二进制数据。为了方便阅读，多个字节之间用空格隔开
    #
    # 11 开头的整数编码如下：
    #     11000000  1 byte  int16_t 类型的整数
    #     11010000  1 byte  int32_t 类型的整数
    #     11100000  1 byte  int64_t 类型的整数
    #     11110000  1 byte  24 bit 有符号整数
    #     11111110  1 byte  8 bit 有符号整数
    #     1111xxxx  1 byte  4 bit 无符号整数，介于 0 至 12 之间
    #
    # content 部分保存着节点的内容，类型和长度由 encoding 和 length 决定
    def read_ziplist_entry(self, f) :
        length = 0
        value = None
        prev_length = read_unsigned_char(f)
        if prev_length == 254 :
            prev_length = read_unsigned_int(f) # TODO 这里是什么情况？读出来，但是又没有用。
        entry_header = read_unsigned_char(f) # 读取一个字节，这里包括encoding和length
        if (entry_header >> 6) == 0 : # encoding = 0 长度小于等于 63 字节的字符数组
            length = entry_header & 0x3F
            value = f.read(length)
        elif (entry_header >> 6) == 1 : # encoding = 1 长度小于等于 16383 字节的字符数组
            length = ((entry_header & 0x3F) << 8) | read_unsigned_char(f)
            value = f.read(length)
        elif (entry_header >> 6) == 2 : #encoding = 2 长度小于等于 4294967295 的字符数组
            length = read_big_endian_unsigned_int(f)
            value = f.read(length)
        # 以下都是encoding = 3的情形
        elif (entry_header >> 4) == 12 : # encoding = 1100 int16_t 类型的整数
            value = read_signed_short(f)
        elif (entry_header >> 4) == 13 : # encoding = 1101 int32_t 类型的整数
            value = read_signed_int(f)
        elif (entry_header >> 4) == 14 : # encoding = 1110 int64_t 类型的整数
            value = read_signed_long(f)
        elif (entry_header == 240) : # encoding = 11110000 24 bit 有符号整数
            value = read_24bit_signed_number(f)
        elif (entry_header == 254) : # encoding = 11111110 8 bit 有符号整数
            value = read_signed_char(f)
        elif (entry_header >= 241 and entry_header <= 253) : # encoding = 1111xxxx 4 bit 无符号整数，介于 0 至 12 之间
            value = entry_header - 241 # 这里处理的貌似很艺术，entry_header在241和253之间，再减241,value刚好在0和12之间
        else :
            raise Exception('read_ziplist_entry', 'Invalid entry_header %d for key %s' % (entry_header, self._key))
        return value

    #   1 byte   1或5 byte    1 byte     总是 255   
    # +--------+-----------+---------+-----------+
    # | zmlen  |    len    |   free  |   zmend   |
    # +--------+-----------+---------+-----------+ 
    # 
    # zmlen : 1字节长，保存zipmap的大小. 如果大于等于254，值不使用。将需要迭代整个zipmap来找出长度
    # len : 后续字符串的长度，可以是键或值的。这个长度存储为1个或5个字节（与上面描述的“长度编码”不同）
    #       如果第一个字节位于0到252，那么它是zipmap的长度。如果第一个字节是253，读取下4个字节作为无符号整数来表示zipmap的长度。254和255 对这个字段是非法的
    # free : 总是1字节，指出值后面的空闲字节数。例如，如果键的值是“America”，更新为“USA”后，将有4个空闲的字节
    # zmend : 总是 255. 指出zipmap结束
    #
    # TODO : 这里不对的啊！应该在len前面还有一个field，记录zipmap中entry个数
    def read_zipmap(self, f) :
        raw_string = self.read_string(f)
        buff = io.BytesIO(bytearray(raw_string))
        num_entries = read_unsigned_char(buff) # 看吧，这里读出来entry个数了吧！
        self._callback.start_hash(self._key, num_entries, self._expiry, info={'encoding':'zipmap', 'sizeof_value':len(raw_string)})
        while True :
            next_length = self.read_zipmap_next_length(buff)
            if next_length is None :
                break
            key = buff.read(next_length)
            next_length = self.read_zipmap_next_length(buff)
            if next_length is None :
                raise Exception('read_zip_map', 'Unexepcted end of zip map for key %s' % self._key)
            free = read_unsigned_char(buff)
            value = buff.read(next_length)
            try:
                value = int(value)
            except ValueError:
                pass

            skip(buff, free)
            self._callback.hset(self._key, key, value)
        self._callback.end_hash(self._key)

    # 如果第一个字节位于 0 到252，那么它是zipmap的长度。如果第一个字节是253，读取下4个字节作为无符号整数来表示zipmap的长度
    # 254 和 255 对这个字段是非法的
    # 
    # TODO : 这又是什么啊！！对不上啊，边界值是253的啊，可是这里怎么变成254了。迷惑啊。
    def read_zipmap_next_length(self, f) :
        num = read_unsigned_char(f)
        if num < 254:
            return num
        elif num == 254:
            return read_unsigned_int(f)
        else:
            return None

    def verify_magic_string(self, magic_string) :
        if magic_string != 'REDIS' :
            raise Exception('verify_magic_string', 'Invalid File Format')

    def verify_version(self, version_str) :
        version = int(version_str)
        if version < 1 or version > 6 :
            raise Exception('verify_version', 'Invalid RDB version number %d' % version)

    def init_filter(self, filters):
        self._filters = {}
        if not filters:
            filters={}

        if not 'dbs' in filters:
            self._filters['dbs'] = None
        elif isinstance(filters['dbs'], int):
            self._filters['dbs'] = (filters['dbs'], )
        elif isinstance(filters['dbs'], list):
            self._filters['dbs'] = [int(x) for x in filters['dbs']]
        else:
            raise Exception('init_filter', 'invalid value for dbs in filter %s' %filters['dbs'])

        if not ('keys' in filters and filters['keys']):
            self._filters['keys'] = re.compile(".*")
        else:
            self._filters['keys'] = re.compile(filters['keys'])

        if not 'types' in filters:
            self._filters['types'] = ('set', 'hash', 'sortedset', 'string', 'list')
        elif isinstance(filters['types'], str):
            self._filters['types'] = (filters['types'], )
        elif isinstance(filters['types'], list):
            self._filters['types'] = [str(x) for x in filters['types']]
        else:
            raise Exception('init_filter', 'invalid value for types in filter %s' %filters['types'])

    def matches_filter(self, db_number, key=None, data_type=None):
        if self._filters['dbs'] and (not db_number in self._filters['dbs']):
            return False
        if key and (not self._filters['keys'].match(str(key))):
            return False

        if data_type is not None and (not self.get_logical_type(data_type) in self._filters['types']):
            return False
        return True

    def get_logical_type(self, data_type):
        return DATA_TYPE_MAPPING[data_type]

    # lzf算法压缩过后的数据格式
    # +----------+----------------+--------------------+
    # | LZF-FLAG | COMPRESSED-LEN | COMPRESSED-CONTENT |
    # +----------+----------------+--------------------+
    def lzf_decompress(self, compressed, expected_length):
        in_stream = bytearray(compressed) #python内置函数
        in_len = len(in_stream)
        in_index = 0
        out_stream = bytearray()
        out_index = 0

        while in_index < in_len :
            ctrl = in_stream[in_index]
            if not isinstance(ctrl, int) :
                raise Exception('lzf_decompress', 'ctrl should be a number %s for key %s' % (str(ctrl), self._key))
            in_index = in_index + 1

            # TODO : 还没有弄清楚COMPRESSED-LEN为何以32为界限, 进行不同的处理
            if ctrl < 32 :
                for x in xrange(0, ctrl + 1) :
                    out_stream.append(in_stream[in_index])
                    #sys.stdout.write(chr(in_stream[in_index]))
                    in_index = in_index + 1
                    out_index = out_index + 1
            else :
                length = ctrl >> 5
                if length == 7 :
                    length = length + in_stream[in_index]
                    in_index = in_index + 1

                ref = out_index - ((ctrl & 0x1f) << 8) - in_stream[in_index] - 1
                in_index = in_index + 1
                for x in xrange(0, length + 2) :
                    out_stream.append(out_stream[ref])
                    ref = ref + 1
                    out_index = out_index + 1
        if len(out_stream) != expected_length :
            raise Exception('lzf_decompress', 'Expected lengths do not match %d != %d for key %s' % (len(out_stream), expected_length, self._key))
        return str(out_stream)

def skip(f, free):
    if free :
        f.read(free)

def ntohl(f) :
    val = read_unsigned_int(f)
    new_val = 0
    new_val = new_val | ((val & 0x000000ff) << 24)
    new_val = new_val | ((val & 0xff000000) >> 24)
    new_val = new_val | ((val & 0x0000ff00) << 8)
    new_val = new_val | ((val & 0x00ff0000) >> 8)
    return new_val

def to_datetime(usecs_since_epoch):
    seconds_since_epoch = usecs_since_epoch / 1000000
    useconds = usecs_since_epoch % 1000000
    # add by liuzi
    econds_since_epoch = 1385967984
    dt = datetime.datetime.utcfromtimestamp(econds_since_epoch)
    delta = datetime.timedelta(microseconds = useconds)
    return dt + delta

def read_signed_char(f) :
    return struct.unpack('b', f.read(1))[0]

def read_unsigned_char(f) :
    return struct.unpack('B', f.read(1))[0]

def read_signed_short(f) :
    return struct.unpack('h', f.read(2))[0]

def read_unsigned_short(f) :
    return struct.unpack('H', f.read(2))[0]

def read_signed_int(f) :
    return struct.unpack('i', f.read(4))[0]

def read_unsigned_int(f) :
    return struct.unpack('I', f.read(4))[0]

def read_big_endian_unsigned_int(f):
    return struct.unpack('>I', f.read(4))[0]

def read_24bit_signed_number(f):
    s = '0' + f.read(3)
    num = struct.unpack('i', s)[0]
    return num >> 8

def read_signed_long(f) :
    return struct.unpack('q', f.read(8))[0]

def read_unsigned_long(f) :
    return struct.unpack('Q', f.read(8))[0]

def string_as_hexcode(string) :
    for s in string :
        if isinstance(s, int) :
            print(hex(s))
        else :
            print(hex(ord(s)))


class DebugCallback(RdbCallback) :
    def start_rdb(self):
        print('[')

    def start_database(self, db_number):
        print('{')

    def set(self, key, value, expiry):
        print('"%s" : "%s"' % (str(key), str(value)))

    def start_hash(self, key, length, expiry):
        print('"%s" : {' % str(key))
        pass

    def hset(self, key, field, value):
        print('"%s" : "%s"' % (str(field), str(value)))

    def end_hash(self, key):
        print('}')

    def start_set(self, key, cardinality, expiry):
        print('"%s" : [' % str(key))

    def sadd(self, key, member):
        print('"%s"' % str(member))

    def end_set(self, key):
        print(']')

    def start_list(self, key, length, expiry):
        print('"%s" : [' % str(key))

    def rpush(self, key, value) :
        print('"%s"' % str(value))

    def end_list(self, key):
        print(']')

    def start_sorted_set(self, key, length, expiry):
        print('"%s" : {' % str(key))

    def zadd(self, key, score, member):
        print('"%s" : "%s"' % (str(member), str(score)))

    def end_sorted_set(self, key):
        print('}')

    def end_database(self, db_number):
        print('}')

    def end_rdb(self):
        print(']')


