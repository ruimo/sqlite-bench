# SQLite benchmark

A microbenchmark for SQLite database. Perform benchmark with the following basic operation. Using Postgres for comparison. A docker image for this benchmark is built on docker hub. You can run the same benchmark on your docker container. No need to install SQLite or Postgres at all.

## Bulk insert

Insert 100000 records. Commit every 100 records.

Command:
```
docker run -t ruimo/sqlite-bench /insertBench.py [--wal] [--copy]
```

Options:
* --wal Use WAL(Write Ahead Log) for SQLite. It dramatically impoves insert/update/remove in SQLite.

* --copy As it is usual to use COPY command instead of SQL insert for bulk insert in Postgres, provided an option to use COPY command.

## Bulk update

Update 50000 records. Commit every 100 records.

Command:
```
docker run -t ruimo/sqlite-bench /updateBench.py [--wal]

Options:
* --wal Use WAL(Write Ahead Log) for SQLite. It dramatically impoves insert/update/remove in SQLite.

## Simple query 1

Perform simple query using date/time function 50000 times.

Command:
```
docker run -t ruimo/sqlite-bench /selectBench.py [--wal]
```

Options:
* --wal Use WAL(Write Ahead Log) for SQLite. It dramatically impoves insert/update/remove in SQLite.

## Simple query 2

Perform simple query using order by and limit 50000 times.

Command:
```
docker run -t ruimo/sqlite-bench /selectBench2.py [--wal]
```

Options:
* --wal Use WAL(Write Ahead Log) for SQLite. It dramatically impoves insert/update/remove in SQLite.

## Inner join

Perform query using inner join 500 times.

Command:
```
docker run -t ruimo/sqlite-bench /selectBench3.py [--wal]
```

## Outer join

Perform query using outer join 500 times.

Command:
```
docker run -t ruimo/sqlite-bench /selectBench3.py [--wal]
```

Options:
* --wal Use WAL(Write Ahead Log) for SQLite. It dramatically impoves insert/update/remove in SQLite.
