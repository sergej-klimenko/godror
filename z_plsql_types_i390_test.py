import oracledb
import time
import os
import psutil
import tracemalloc

# Set up your Oracle connection string
dsn = os.environ.get("ORACLE_DSN", "")

def create_types(conn):
    ddl = [
        """create or replace type pair force as object (
            key    number(3),
            value  number(18)
        )""",
        "create or replace type pair_list force as table of pair",
        """create or replace type pobj force as object (
            id    number(10),
            pairs pair_list
        )""",
        "create or replace type pobj_t is table of pobj",
        """CREATE OR REPLACE PACKAGE test_pkg_sample AS
            PROCEDURE test_pobj_in (
                recs IN OUT pobj_t
            );
        END test_pkg_sample;""",
        """CREATE OR REPLACE PACKAGE BODY test_pkg_sample AS
            PROCEDURE test_pobj_in (
                recs IN OUT pobj_t
            ) IS
            BEGIN
                FOR i IN 1 .. recs.COUNT LOOP
                    recs(i).id := recs(i).id + 1;
                    FOR j IN 1 .. recs(i).pairs.COUNT LOOP
                        recs(i).pairs(j).value := recs(i).pairs(j).value + 10;
                    END LOOP;
                END LOOP;
            END test_pobj_in;
        END test_pkg_sample;"""
    ]
    with conn.cursor() as cur:
        for stmt in ddl:
            try:
                cur.execute(stmt)
            except oracledb.DatabaseError as e:
                print(f"Error executing DDL: {e}")

def drop_types(conn):
    ddl = [
        "DROP PACKAGE test_pkg_sample",
        "DROP TYPE pobj_t",
        "DROP TYPE pobj",
        "DROP TYPE pair_list",
        "DROP TYPE pair"
    ]
    with conn.cursor() as cur:
        for stmt in ddl:
            try:
                cur.execute(stmt)
            except oracledb.DatabaseError as e:
                print(f"Error dropping type: {e}")

def make_obj_list(conn, nobjs, npairs):
    pair_type = conn.gettype("PAIR")
    pair_list_type = conn.gettype("PAIR_LIST")
    pobj_type = conn.gettype("POBJ")
    pobj_t_type = conn.gettype("POBJ_T")

    pobj_list = pobj_t_type.newobject()
    for i in range(nobjs):
        pobj = pobj_type.newobject()
        pobj.ID = i + 1
        pairs = pair_list_type.newobject()
        for j in range(npairs):
            pair = pair_type.newobject()
            pair.KEY = j + 1
            pair.VALUE = (i + 1) * 1000 + (j + 1)
            pairs.append(pair)
        pobj.PAIRS = pairs
        pobj_list.append(pobj)
    return pobj_list

def print_stats(loop_cnt, start_mem):
    process = psutil.Process(os.getpid())
    rss = process.memory_info().rss
    print(f"{loop_cnt}; process memory (rss): {rss / (1 << 20):.3f} MiB")
    if start_mem and rss > start_mem * 2:
        print(f"Memory usage doubled: started with {start_mem / (1 << 20):.3f} MiB, now {rss / (1 << 20):.3f} MiB")

def main():
    oracledb.init_oracle_client()
    conn = oracledb.connect(dsn)
    try:
        create_types(conn)
        step = 100
        nobjs = step
        npairs = step // 2
        start_mem = None
        loop_cnt = 0

        deadline = time.time() + 50  # 50 seconds

        while time.time() < deadline:
            pobj_list = make_obj_list(conn, nobjs, npairs)
            with conn.cursor() as cur:
                cur.callproc("test_pkg_sample.test_pobj_in", [pobj_list])
            if start_mem is None:
                process = psutil.Process(os.getpid())
                start_mem = process.memory_info().rss
            if loop_cnt % step == 0:
                print_stats(loop_cnt, start_mem)
            loop_cnt += 1

        print_stats(loop_cnt, start_mem)
    finally:
        drop_types(conn)
        conn.close()

if __name__ == "__main__":
    main()