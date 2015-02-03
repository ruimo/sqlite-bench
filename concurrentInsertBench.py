"""Perform concurrent insert benchmark test.

usage: concurrentInsertBench.py [-h] [--wal]

options:
    -h, --help  Show this help message and exit
    --wal       Use WAL(Write a head log) instead of traditional rollback journal for SQLite.
"""

from docopt import docopt
import io, datetime, bench, time
import threading

N = 300

def doInTransaction(conn, beginTranFunc, commitTranFunc, func):
    cur = conn.cursor()
    beginTranFunc(cur)
    ret = func(cur)
    commitTranFunc(cur)
    return ret

def doInTransactionSqlite(conn, func):
    return doInTransaction(
        conn, lambda cur: cur.execute('BEGIN TRANSACTION'), lambda cur: cur.execute('COMMIT'), func
    )

def doInTransactionPgsql(conn, func):
    return doInTransaction(
        conn, (lambda cur: None), lambda cur: conn.commit(), func
    )
    
def pickAddr(cur):
    cur.execute("select address_id from addresses order by RANDOM() limit 1")
    return cur.fetchone()

def pickUser(cur):
    cur.execute("select user_id from users order by RANDOM() limit 1")
    return cur.fetchone()

def pickUserDept(cur):
    cur.execute(
        "select user_id, department_id from users u cross join departments d " +
        "where not exists (select 'X' from user_department where user_id = u.user_id and department_id = d.department_id) " +
        "order by RANDOM() limit 1"
    )
    return cur.fetchone()

def insertDepartmentSqlite(conn):
    def insertFunc(cur, i):
        cur.execute(
            "insert into departments (department_name, created) values (?, CURRENT_TIMESTAMP)", 
            ("dept%08d" % i, )
        )

    def performer():
        for i in range(0, N):
            doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i))

    bench.withStopwatch("insert departments with SQLite", performer)

def insertAddressSqlite(conn):
    def insertFunc(cur, i):
        cur.execute(
            "insert into addresses (address) values (?)", ("addr%08d" % i, )
        )

    def performer():
        for i in range(0, N):
            doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i))

    bench.withStopwatch("insert addresses with SQLite", performer)

def insertUserSqlite(conn):
    def insertFunc(cur, i, keys):
        cur.execute(
            "insert into users(address_id, user_name, first_name, last_name, created) " +
            "values (?, ?, ?, ?, CURRENT_TIMESTAMP)",
            (keys[0], "user%08d" % i, "fname%08d" % i, "lname%08d" % i)
        )

    def getKeys():
        keys = doInTransactionSqlite(conn, pickAddr)
        if keys is None:
            time.sleep(0.3)
            return getKeys()
        return keys

    def performer():
        for i in range(0, N):
            keys = getKeys()
            doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i, keys))

    bench.withStopwatch("insert users with SQLite", performer)

def insertUserDepartmentSqlite(conn):
    def insertFunc(cur, i, keys):
        cur.execute(
            "insert into user_department(user_id, department_id) values (?, ?)", (keys[0], keys[1])
        )

    def getKeys():
        keys = doInTransactionSqlite(conn, pickUserDept)
        if keys is None:
            time.sleep(0.3)
            return getKeys()
        return keys

    def performer():
        for i in range(0, N):
            keys = getKeys()
            doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i, keys))

    bench.withStopwatch("insert user_department with SQLite", performer)

def insertDepartmentPgsql(conn):
    def insertFunc(cur, i):
        cur.execute(
            "insert into departments (department_name, created) values (%s, CURRENT_TIMESTAMP)",
            ("dept%08d" % i, )
        )

    def performer():
        for i in range(0, N):
            doInTransactionPgsql(conn, lambda cur: insertFunc(cur, i))

    bench.withStopwatch("insert departments with Postgres", performer)

def insertAddressPgsql(conn):
    def insertFunc(cur, i):
        cur.execute(
            "insert into addresses (address) values (%s)", ("addr%08d" % i, )
        )

    def performer():
        for i in range(0, N):
            doInTransactionPgsql(conn, lambda cur: insertFunc(cur, i))

    bench.withStopwatch("insert addresses with Postgres", performer)

def insertUserPgsql(conn):
    def insertFunc(cur, i, keys):
        cur.execute(
            "insert into users(address_id, user_name, first_name, last_name, created) " +
            "values (%s, %s, %s, %s, CURRENT_TIMESTAMP)",
            (keys[0], "user%08d" % i, "fname%08d" % i, "lname%08d" % i)
        )

    def getKeys():
        keys = doInTransactionPgsql(conn, pickAddr)
        if keys is None:
            time.sleep(0.3)
            return getKeys()
        return keys

    def performer():
        for i in range(0, N):
            keys = getKeys()
            doInTransactionPgsql(conn, lambda cur: insertFunc(cur, i, keys))

    bench.withStopwatch("insert users with Postgres", performer)

def doWithThreadSqlite(func):
    t = threading.Thread(
        target = lambda : bench.withSqliteConnection("/tmp/test.db", func, isolationLevel = None, useWal = args["--wal"])
    )
    t.start()
    return t

def doWithThreadPgsql(func):
    t = threading.Thread(target = lambda: bench.withPgsqlConnection(func))
    t.start()
    return t

def sqliteBench(args):
    doWithThreadSqlite(bench.createTableSqlite).join()
    depThread = doWithThreadSqlite(insertDepartmentSqlite)
    addrThread = doWithThreadSqlite(insertAddressSqlite)
    userThread = doWithThreadSqlite(insertUserSqlite)
    userDeptThread = doWithThreadSqlite(insertUserDepartmentSqlite)
    depThread.join()
    addrThread.join()
    userThread.join()
    userDeptThread.join()

def pgsqlBench(args):
    bench.withPgsqlConnection(bench.createTablePgsql)
    depThread = doWithThreadPgsql(insertDepartmentPgsql)
    addrThread = doWithThreadPgsql(insertAddressPgsql)
    userThread = doWithThreadPgsql(insertUserPgsql)
    depThread.join()
    addrThread.join()
    userThread.join()

if __name__ == '__main__':
    args = docopt(__doc__)
    # 'isolationLevel = None' means auto commit.
    bench.withStopwatch("SQLite all", lambda: sqliteBench(args))
    bench.withStopwatch("Postgres all", lambda: pgsqlBench(args))
