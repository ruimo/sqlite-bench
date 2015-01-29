#!/bin/sh

# Re-generate Postgres database space.
su - postgres -c "mkdir /tmp/pgsql-bench"
su - postgres -c "/usr/lib/postgresql/9.3/bin/initdb -D /tmp/pgsql-bench"

# Change port to prevent existing other postgres intances to cause conflict.
sed -i -e 's/^#port =.*$/port = 5431/' /tmp/pgsql-bench/postgresql.conf
sed -i -e "s|^#unix_socket_directories.*|unix_socket_directories = '/tmp'|" /tmp/pgsql-bench/postgresql.conf
chown postgres /tmp/pgsql-bench/postgresql.conf

# launch postgres
su - postgres -c "nohup /usr/lib/postgresql/9.3/bin/postgres -D /tmp/pgsql-bench" &
echo $! > run.pid
sleep 5

# Create database
su - postgres -c "createdb -h /tmp -p 5431 --template=template0 -E UTF-8 testDb"

# Start benchmark
su - postgres -c "python3 $*"
