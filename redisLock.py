#coding:utf8
import uuid
import time
import math
import redis
default_conn = redis.StrictRedis.from_url("redis://localhost:6379/1")

def acquire_lock(conn=None, lockname='', acquire_timeout=20):
    '''使用redis实现最简单的锁
    使用setnx命令设置锁的值， 设置成功即为获取锁成功    
    '''
    conn = conn or default_conn
    identifier = str(uuid.uuid4())
    end_time = time.time() + acquire_timeout
    while time.time() < end_time:
        if conn.setnx("lock:" + lockname, identifier):
            return identifier
        time.sleep(.001)
    return False


def release_lock(conn=None, lockname='', identifier=''):
    conn = conn or default_conn
    pipe = conn.pipeline(True)
    lockname = "lock:" + lockname
    while 1:
        try:
            pipe.watch(lockname)
            if conn.get(lockname) == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True
            pipe.unwatch()
            break
        except redis.exceptions.WatchError:
            pass
    return False

def acquire_lock_with_timeout(conn=None, lockname='', acquire_timeout=20, lock_timeout=10):
    '''带有超时限制特性的锁'''
    conn = conn or default_conn
    identifier = str(uuid.uuid4())
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))
    end_time = time.time() + acquire_timeout
    while time.time() < end_time:
        if conn.setnx(lockname, identifier):
            conn.expire(lockname, lock_timeout)
            return identifier
        elif not conn.ttl(lockname):
            conn.expire(lockname, lock_timeout)
        time.sleep(.001)
    return False

release_lock_with_timeout = release_lock

def acquire_semaphore(conn=None, semname='', limit=5, timeout=10):
    conn = conn or default_conn
    identifier = str(uuid.uuid4())
    semname = 'lock:zset:' + semname
    now = time.time()
    pipe = conn.pipeline(True)
    pipe.zremrangebyscore(semname, '-inf', now-timeout)
    pipe.zadd(semname, now, identifier)
    pipe.zrank(semname, identifier)
    result = pipe.execute()
    #print result
    if result[-1] < limit:
        return identifier
    conn.zrem(semname, identifier)
    return None

def release_semaphore(conn=None, semname='', identifier=''):
    conn = conn or default_conn
    semname = 'lock:zset:' + semname
    return conn.zrem(semname, identifier)
    

def test():
    lockname = "test"
    acquire, release = acquire_lock, release_lock
    acquire, release = acquire_lock_with_timeout, release_lock_with_timeout
    acquire, release = acquire_semaphore, release_semaphore
    def work(index):
        identifier = acquire(default_conn, lockname)
        if identifier:
            print "ok!!!  %s"%index
            release(default_conn, lockname, identifier)
        else:
            print "fail!!!  %s"%index
        return 
    from multiprocessing import dummy
    pool = dummy.Pool(100)
    pool.map(work, range(100))
    #pool.join()
    return



if __name__ == "__main__":
    test()