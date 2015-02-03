import sqlite3, time, psycopg2

def withSqliteConnection(dbFileName, f, isolationLevel, useWal = False, timeout = 60):
    try:
        conn = sqlite3.connect(dbFileName, timeout)
        conn.isolation_level = isolationLevel
        if useWal:
            print('Using WAL...')
            conn.execute("PRAGMA journal_mode=WAL")
        else:
            print('Using rollback journal...')
        f(conn)
    finally:
        if conn:
            conn.close()

def withPgsqlConnection(f):
    try:
        conn = psycopg2.connect(
            database = "testDb", port = "5431", host = "/tmp"
        )
        f(conn)
    finally:
        if conn:
            conn.close()

def createTableSqlite(conn):
    primariKeySpec = "integer primary key autoincrement"
    conn.execute("""
      PRAGMA foreign_keys = ON
    """)
    cur = conn.cursor()
    cur.execute("""
      create table departments (
        department_id %s,
        department_name text unique not null,
        created text not null
      )
    """ % primariKeySpec)
    createTableCommon(conn, primariKeySpec)

def createTablePgsql(conn):
    primariKeySpec = "serial primary key"
    cur = conn.cursor()
    cur.execute("""
      create table departments (
        department_id %s,
        department_name text unique not null,
        created timestamp not null
      )
    """ % primariKeySpec)
    createTableCommon(conn, primariKeySpec)
    conn.commit()

def createTableCommon(conn, primariKeySpec):
    cur = conn.cursor()
    cur.execute("""
      create table addresses (
        address_id %s,
        address text unique not null
      )
    """ % primariKeySpec)
    cur.execute("""
      create table users (
        user_id %s,
        address_id integer not null,
        user_name text unique not null,
        first_name text not null,
        last_name text not null,
        created text not null,
        foreign key (address_id) references addresses(address_id)
      )
    """ % primariKeySpec)
    cur.execute("""
      create table user_department (
        user_department_id %s,
        user_id integer not null,
        department_id integer not null,
        unique ( user_id, department_id),
        foreign key (user_id) references users(user_id),
        foreign key (department_id) references departments(department_id)
      )
    """ % primariKeySpec)
    cur.execute("""
      create index department_created on departments ( created )
    """)

def withStopwatch(title, f):
    start = time.time()
    print('%s started...' % title)
    f()
    print('%s %.3f secs' % (title, time.time() - start))
