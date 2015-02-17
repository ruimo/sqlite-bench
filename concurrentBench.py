"""Perform concurrent insert/query benchmark test.

usage: concurrentBench.py [-h] [--wal]

options:
    -h, --help  Show this help message and exit
    --wal       Use WAL(Write a head log) instead of traditional rollback journal for SQLite.
"""

from docopt import docopt
import io, datetime, bench, time
import threading

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
    
def getKeysSqlite(conn, picker):
    keys = doInTransactionSqlite(conn, picker)
    if keys is None:
        time.sleep(0.3)
        return getKeysSqlite(conn, picker)
    return keys

def getKeysPgsql(conn, picker):
    keys = doInTransactionPgsql(conn, picker)
    if keys is None:
        time.sleep(0.3)
        return getKeysPgsql(conn, picker)
    return keys

def addrPicker(cur):
    cur.execute("select address_id from addresses order by RANDOM() limit 1")
    return cur.fetchone()

def userDeptPicker(cur):
    cur.execute(
        "select user_id, department_id from users u cross join departments d " +
        "where not exists (select 'X' from user_department where user_id = u.user_id and department_id = d.department_id) " +
        "order by RANDOM() limit 1"
    )
    return cur.fetchone()

def insertDepartmentSqlite(conn, doNtimes):
    def insertFunc(cur, i):
        cur.execute(
            "insert into departments (department_name, created) values (?, CURRENT_TIMESTAMP)", 
            ("dept%08d" % i, )
        )

    bench.withStopwatch(
        "insert departments with SQLite",
        lambda: doNtimes(lambda i: doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i)))
    )

def insertAddressSqlite(conn, doNtimes):
    def insertFunc(cur, i):
        cur.execute(
            "insert into addresses (address) values (?)", ("addr%08d" % i, )
        )

    bench.withStopwatch(
        "insert addresses with SQLite",
        lambda: doNtimes(lambda i: doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i)))
    )

def insertUserSqlite(conn, doNtimes):
    def insertFunc(cur, i, keys):
        cur.execute(
            "insert into users(address_id, user_name, first_name, last_name, created) " +
            "values (?, ?, ?, ?, CURRENT_TIMESTAMP)",
            (keys[0], "user%08d" % i, "fname%08d" % i, "lname%08d" % i)
        )

    def performer(i):
        keys = getKeysSqlite(conn, addrPicker)
        doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i, keys))

    bench.withStopwatch("insert users with SQLite", lambda: doNtimes(performer))

def insertUserDepartmentSqlite(conn, doNtimes):
    def insertFunc(cur, i, keys):
        cur.execute(
            "insert into user_department(user_id, department_id) values (?, ?)", (keys[0], keys[1])
        )

    def performer(i):
        keys = getKeysSqlite(conn, userDeptPicker)
        doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i, keys))

    bench.withStopwatch(
        "insert user_department with SQLite", lambda: doNtimes(performer)
    )

def insertDepartmentPgsql(conn, doNtimes):
    def insertFunc(cur, i):
        cur.execute(
            "insert into departments (department_name, created) values (%s, CURRENT_TIMESTAMP)",
            ("dept%08d" % i, )
        )

    def performer(i):
        doInTransactionPgsql(conn, lambda cur: insertFunc(cur, i))

    bench.withStopwatch("insert departments with Postgres", lambda: doNtimes(performer))

def insertAddressPgsql(conn, n):
    def insertFunc(cur, i):
        cur.execute(
            "insert into addresses (address) values (%s)", ("addr%08d" % i, )
        )

    def performer(i):
        doInTransactionPgsql(conn, lambda cur: insertFunc(cur, i))

    bench.withStopwatch("insert addresses with Postgres", lambda: doNtimes(performer))

def insertUserPgsql(conn, doNtimes):
    def insertFunc(cur, i, keys):
        cur.execute(
            "insert into users(address_id, user_name, first_name, last_name, created) " +
            "values (%s, %s, %s, %s, CURRENT_TIMESTAMP)",
            (keys[0], "user%08d" % i, "fname%08d" % i, "lname%08d" % i)
        )

    def performer(i):
        keys = getKeysPgsql(conn, addrPicker)
        doInTransactionPgsql(conn, lambda cur: insertFunc(cur, i, keys))

    bench.withStopwatch("insert users with Postgres", lambda: doNtimes(performer))

def insertUserDepartmentPgsql(conn, doNtimes):
    def insertFunc(cur, i, keys):
        cur.execute(
            "insert into user_department(user_id, department_id) values (%s, %s)", (keys[0], keys[1])
        )

    def performer(i):
        keys = getKeysPgsql(conn, userDeptPicker)
        doInTransactionSqlite(conn, lambda cur: insertFunc(cur, i, keys))

    bench.withStopwatch("insert user_department with Postgres", lambda: doNtimes(performer))

def loop(func, n):
    for i in range(0, n):
        func(i)

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

def queryDepartmentSqlite(conn, doNtimes):
    cur = conn.cursor()
    def performer(i):
        cur.execute("select max(created) from departments where created < datetime(CURRENT_TIMESTAMP, '-10 seconds')")
        cur.fetchall()
    bench.withStopwatch("SQLite query department", lambda: doNtimes(performer))

def queryDepartmentPgsql(conn, doNtimes):
    cur = conn.cursor()
    def performer(i):
        cur.execute("select max(created) from departments where created < CURRENT_TIMESTAMP + '-10 seconds'")
        cur.fetchall()
    bench.withStopwatch("Postgres query department", lambda: doNtimes(performer))

def queryUserSqlite(conn, doNtimes):
    cur = conn.cursor()
    def performer(i):
        cur.execute("select * from users order by user_name limit 1 offset random()")
        cur.fetchall()
    bench.withStopwatch("SQLite query user", lambda: doNtimes(performer))

def queryUserPgsql(conn, doNtimes):
    cur = conn.cursor()
    def performer(i):
        cur.execute("select * from users order by user_name limit 1 offset random()")
        cur.fetchall()
    bench.withStopwatch("Postgres query user", lambda: doNtimes(performer))

def queryAddressSqlite(conn, doNtimes):
    cur = conn.cursor()
    def performer(i):
        cur.execute("select * from addresses order by address limit 1 offset random()")
        cur.fetchall()
    bench.withStopwatch("SQLite query address", lambda: doNtimes(performer))

def queryAddressPgsql(conn, doNtimes):
    cur = conn.cursor()
    def performer(i):
        cur.execute("select * from addresses order by address limit 1 offset random()")
        cur.fetchall()
    bench.withStopwatch("Postgres query address", lambda: doNtimes(performer))

def queryUserDepartmentSqlite(conn, doNtimes):
    cur = conn.cursor()
    def performer(i):
        cur.execute("select * from user_department order by user_id limit 1 offset random()")
        cur.fetchall()
    bench.withStopwatch("SQLite query user department", lambda: doNtimes(performer))

def queryUserDepartmentPgsql(conn, doNtimes):
    cur = conn.cursor()
    def performer(i):
        cur.execute("select * from user_department order by user_id limit 1 offset random()")
        cur.fetchall()
    bench.withStopwatch("Postgres query user department", lambda: doNtimes(performer))

def doInThreads(*funcs):
    threads = []
    for f in funcs:
        threads.append(f())
    for t in threads:
        t.join()

def sqliteQueryBench(args, doNtimes):
    doInThreads(
        lambda: doWithThreadSqlite(lambda conn: queryDepartmentSqlite(conn, doNtimes)),
        lambda: doWithThreadSqlite(lambda conn: queryAddressSqlite(conn, doNtimes)),
        lambda: doWithThreadSqlite(lambda conn: queryUserSqlite(conn, doNtimes)),
        lambda: doWithThreadSqlite(lambda conn: queryUserDepartmentSqlite(conn, doNtimes))
    )

def sqliteUpdateBench(args, doNtimes):
    doWithThreadSqlite(bench.createTableSqlite).join()
    doInThreads(
        lambda: doWithThreadSqlite(lambda conn: insertDepartmentSqlite(conn, doNtimes)),
        lambda: doWithThreadSqlite(lambda conn: insertAddressSqlite(conn, doNtimes)),
        lambda: doWithThreadSqlite(lambda conn: insertUserSqlite(conn, doNtimes)),
        lambda: doWithThreadSqlite(lambda conn: insertUserDepartmentSqlite(conn, doNtimes))
    )

def pgsqlQueryBench(args, doNtimes):
    doInThreads(
        lambda: doWithThreadPgsql(lambda conn: queryDepartmentPgsql(conn, doNtimes)),
        lambda: doWithThreadPgsql(lambda conn: queryAddressPgsql(conn, doNtimes)),
        lambda: doWithThreadPgsql(lambda conn: queryUserPgsql(conn, doNtimes)),
        lambda: doWithThreadPgsql(lambda conn: queryUserDepartmentPgsql(conn, doNtimes))
    )

def pgsqlUpdateBench(args, doNtimes):
    doWithThreadPgsql(bench.createTablePgsql).join()
    doInThreads(
        lambda: doWithThreadPgsql(lambda conn: insertDepartmentPgsql(conn, doNtimes)),
        lambda: doWithThreadPgsql(lambda conn: insertAddressPgsql(conn, doNtimes)),
        lambda: doWithThreadPgsql(lambda conn: insertUserPgsql(conn, doNtimes)),
        lambda: doWithThreadPgsql(lambda conn: insertUserDepartmentPgsql(conn, doNtimes))
    )

if __name__ == '__main__':
    args = docopt(__doc__)

    doNtimes = lambda func: loop(func, 300)
    bench.withStopwatch("SQLite update all", lambda: sqliteUpdateBench(args, doNtimes))
    bench.withStopwatch("Postgres update all", lambda: pgsqlUpdateBench(args, doNtimes))

    doNtimes = lambda func: loop(func, 50000)
    bench.withStopwatch("SQLite query all", lambda: sqliteQueryBench(args, doNtimes))
    bench.withStopwatch("Postgres query all", lambda: pgsqlQueryBench(args, doNtimes))

