import sqlite3
import os

#SCRIPTS
creation_script="""
CREATE TABLE gcvb(id            INTEGER PRIMARY KEY,
                  command_line  TEXT,
                  creation_date INTEGER);

CREATE TABLE run(id         INTEGER PRIMARY KEY,
                 start_date INTEGER,
                 end_date   INTEGER,
                 gcvb_id    INTEGER,
                 FOREIGN KEY(gcvb_id) REFERENCES gcvb(id));

CREATE TABLE test(id         INTEGER PRIMARY KEY,
                  name       TEXT,
                  start_date INTEGER,
                  end_date   INTEGER,
                  run_id     INTEGER,
                  FOREIGN KEY(run_id) REFERENCES run(id));

CREATE TABLE valid(id      INTEGER PRIMARY KEY,
                   metric  TEXT,
                   value   REAL,
                   test_id INTEGER,
                   FOREIGN KEY(test_id) REFERENCES test(id));

CREATE TABLE exec(id           INTEGER PRIMARY KEY,
                  exec_name    TEXT,                      -- from config.yaml
                  code_version TEXT,
                  run_id       INTEGER,
                  FOREIGN KEY(run_id) REFERENCES run(id));
"""

#GLOBAL
database="gcvb.db"

def connect(file,f, *args, **kwargs):
    conn=sqlite3.connect(file)
    conn.row_factory=sqlite3.Row
    c=conn.cursor()
    try:
        res = f(c, *args, **kwargs) #supposed to contain execute statements.
    except:
        conn.rollback()
        raise
    else:
        conn.commit()
    finally:
        conn.close()
    return res


def with_connection(f):
    """decorator for function needing to connect to the database"""
    def with_connection_(*args, **kwargs):
        return connect(database,f, *args, **kwargs)
    return with_connection_

def connection_from_computation_directory(f):
    """decorator for function connecting from the computation directory"""
    def fun_wrapper(*args, **kwargs):
        return connect(os.path.join("..","..","..",database),f, *args, **kwargs)
    return fun_wrapper

@with_connection
def create_db(cursor):
    cursor.executescript(creation_script)

@with_connection
def new_gcvb_instance(cursor, command_line):
    cursor.execute("INSERT INTO gcvb(command_line,creation_date) VALUES (?,CURRENT_TIMESTAMP)",[command_line])
    return cursor.lastrowid

@with_connection
def get_last_gcvb(cursor):
    cursor.execute("SELECT * from gcvb ORDER BY creation_date DESC LIMIT 1")
    return cursor.fetchone()["id"]

@with_connection
def add_run(cursor, gcvb_id):
    cursor.execute("INSERT INTO run(gcvb_id) VALUES (?)",[gcvb_id])
    return cursor.lastrowid

@with_connection
def add_tests(cursor, run, test_list):
    tests=[(t["id"],run) for t in test_list]
    cursor.executemany("INSERT INTO test(name,run_id) VALUES(?,?)",tests)

@connection_from_computation_directory
def start_test(cursor,run,test_id):
    cursor.execute("""UPDATE test
                      SET start_date = CURRENT_TIMESTAMP
                      WHERE name = ? AND run_id = ?""",[test_id,run])

@connection_from_computation_directory
def end_test(cursor, run, test_id):
    cursor.execute("""UPDATE test
                      SET end_date = CURRENT_TIMESTAMP
                      WHERE name = ? AND run_id = ?""",[test_id,run])

@connection_from_computation_directory
def add_metric(cursor, name, value):
    for env in ["GCVB_RUN_ID","GCVB_TEST_ID"]:
        if env not in os.environ:
            raise Exception("Environment variable {} is not defined.".format(env))
    test_id=os.environ["GCVB_TEST_ID"] # string as in the yaml file.
    run_id=os.environ["GCVB_RUN_ID"] # integer id
    cursor.execute("""SELECT id FROM test
                      WHERE name = ? AND run_id = ?""",[test_id,run_id])
    test_id=cursor.fetchone()["id"] #now an int
    cursor.execute("INSERT INTO valid(metric,value,test_id) VALUES (?,?,?)",[name,value,test_id])