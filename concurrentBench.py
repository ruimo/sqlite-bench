"""Perform concurrent insert/query benchmark test.

usage: concurrentBench.py [-h] [--wal]

options:
    -h, --help  Show this help message and exit
    --wal       Use WAL(Write a head log) instead of traditional rollback journal for SQLite.
"""

from docopt import docopt
import io, datetime, bench, time
import threading

N = 300
N2 = 50000

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

def insertUserDepartmentPgsql(conn):
    def insertFunc(cur, i, keys):
        cur.execute(
            "insert into user_department(user_id, department_id) values (%s, %s)", (keys[0], keys[1])
        )

    def getKeys():
        keys = doInTransactionPgsql(conn, pickUserDept)
        if keys is None:
            time.sleep(0.3)
            return getKeys()
        return keys

    def performer():
        for i in range(0, N):
            keys = getKeys()
            doInTransactionPgsql(conn, lambda cur: insertFunc(cur, i, keys))

    bench.withStopwatch("insert user_department with Postgres", performer)

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

def queryDepartmentSqlite(conn):
    def performer():
        cur = conn.cursor()
        for i in range(1, N2):
            cur.execute("select max(created) from departments where created < datetime(CURRENT_TIMESTAMP, '-10 seconds')")
            cur.fetchall()
    bench.withStopwatch("SQLite query department", performer)

def queryDepartmentPgsql(conn):
    def performer():
        cur = conn.cursor()
        for i in range(1, N2):
            cur.execute("select max(created) from departments where created < CURRENT_TIMESTAMP + '-10 seconds'")
            cur.fetchall()
    bench.withStopwatch("Postgres query department", performer)

def queryUserSqlite(conn):
    def performer():
        cur = conn.cursor()
        for i in range(1, N2):
            cur.execute("select * from users order by user_name limit 1 offset random()")
            cur.fetchall()
    bench.withStopwatch("SQLite query user", performer)

def queryUserPgsql(conn):
    def performer():
        cur = conn.cursor()
        for i in range(1, N2):
            cur.execute("select * from users order by user_name limit 1 offset random()")
            cur.fetchall()
    bench.withStopwatch("Postgres query user", performer)

def queryAddressSqlite(conn):
    def performer():
        cur = conn.cursor()
        for i in range(1, N2):
            cur.execute("select * from addresses order by address limit 1 offset random()")
            cur.fetchall()
    bench.withStopwatch("SQLite query address", performer)

def queryAddressPgsql(conn):
    def performer():
        cur = conn.cursor()
        for i in range(1, N2):
            cur.execute("select * from addresses order by address limit 1 offset random()")
            cur.fetchall()
    bench.withStopwatch("Postgres query address", performer)

def queryUserDepartmentSqlite(conn):
    def performer():
        cur = conn.cursor()
        for i in range(1, N2):
            cur.execute("select * from user_department order by user_id limit 1 offset random()")
            cur.fetchall()
    bench.withStopwatch("SQLite query user department", performer)

def queryUserDepartmentPgsql(conn):
    def performer():
        cur = conn.cursor()
        for i in range(1, N2):
            cur.execute("select * from user_department order by user_id limit 1 offset random()")
            cur.fetchall()
    bench.withStopwatch("Postgres query user department", performer)

def sqliteUpdateBench(args):
    doWithThreadSqlite(bench.createTableSqlite).join()
    depThread = doWithThreadSqlite(insertDepartmentSqlite)
    addrThread = doWithThreadSqlite(insertAddressSqlite)
    userThread = doWithThreadSqlite(insertUserSqlite)
    userDeptThread = doWithThreadSqlite(insertUserDepartmentSqlite)
    depThread.join()
    addrThread.join()
    userThread.join()
    userDeptThread.join()

def sqliteQueryBench(args):
    depThread = doWithThreadSqlite(queryDepartmentSqlite)
    userThread = doWithThreadSqlite(queryUserSqlite)
    addressThread = doWithThreadSqlite(queryAddressSqlite)
    userDepartmentThread = doWithThreadSqlite(queryUserDepartmentSqlite)
    depThread.join()
    userThread.join()
    addressThread.join()
    userDepartmentThread.join()

def sqliteBench(args):
    bench.withStopwatch("SQLite update all", lambda: sqliteUpdateBench(args))
    bench.withStopwatch("SQLite query all", lambda: sqliteQueryBench(args))

def pgsqlUpdateBench(args):
    bench.withPgsqlConnection(bench.createTablePgsql)
    depThread = doWithThreadPgsql(insertDepartmentPgsql)
    addrThread = doWithThreadPgsql(insertAddressPgsql)
    userThread = doWithThreadPgsql(insertUserPgsql)
    userDeptThread = doWithThreadPgsql(insertUserDepartmentPgsql)
    depThread.join()
    addrThread.join()
    userThread.join()
    userDeptThread.join()

def pgsqlQueryBench(args):
    depThread = doWithThreadPgsql(queryDepartmentPgsql)
    addrThread = doWithThreadPgsql(queryAddressPgsql)
    userThread = doWithThreadPgsql(queryUserPgsql)
    userDeptThread = doWithThreadPgsql(queryUserDepartmentPgsql)
    depThread.join()
    addrThread.join()
    userThread.join()
    userDeptThread.join()

def pgsqlBench(args):
    bench.withStopwatch("Postgres update all", lambda: pgsqlUpdateBench(args))
    bench.withStopwatch("Postgres query all", lambda: pgsqlQueryBench(args))

if __name__ == '__main__':
    args = docopt(__doc__)
    sqliteBench(args)
    pgsqlBench(args)
