from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


class SQLRet:
    def __init__(self, ret_val=None, rows_affected=None, result=None):
        self.ret_val = ret_val
        self.rows_affected = rows_affected
        self.result = result


def sql_command(query, printSchema=False, to_commit=True):
    conn = None
    rows_effected = None
    result = None
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(query, printSchema)
        if to_commit:
            conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        if not to_commit:
            conn.rollback()
        print(e)
        return SQLRet(ReturnValue.ERROR)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        if not to_commit:
            conn.rollback()
        print(e)
        return SQLRet(ReturnValue.BAD_PARAMS)
    except DatabaseException.CHECK_VIOLATION as e:
        if not to_commit:
            conn.rollback()
        print(e)
        return SQLRet(ReturnValue.BAD_PARAMS)
    except DatabaseException.UNIQUE_VIOLATION as e:
        if not to_commit:
            conn.rollback()
        print(e)
        return SQLRet(ReturnValue.ALREADY_EXISTS)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        if not to_commit:
            conn.rollback()
        print(e)
        return SQLRet(ReturnValue.NOT_EXISTS)  # TODO - am i right?
        # return ReturnValue.ERROR  # TODO - notice if this is correct
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return SQLRet(ReturnValue.OK, rows_effected, result)


def insert(table, obj):
    # TODO - SQL action to add the query ↓
    return sql_command("INSERT INTO {} values ({})".format(table, str((obj.__dict__.values()))[13:-2])).ret_val
    # the above translates to INSERT INTO <tabke name> values (<values of the obj>)

    # if ret.rows_affected == 0:  # lookup queryID to check that it's unique and not in DB yet
    #     return ReturnValue.ALREADY_EXISTS
    # else:
    #     return ret.ret_val


def createTables():
    sql_command("CREATE TABLE TQuery(queryID INTEGER PRIMARY KEY NOT NULL UNIQUE CHECK(queryID > 0),\
                                    purpose TEXT NOT NULL,\
                                    size INTEGER NOT NULL CHECK(size >= 0));\
                \
                CREATE TABLE TRAM(ramID INTEGER PRIMARY KEY NOT NULL UNIQUE CHECK(ramID > 0),\
                                    company TEXT NOT NULL,\
                                    size INTEGER NOT NULL CHECK(size > 0));\
                \
                CREATE TABLE TDisk(diskID INTEGER PRIMARY KEY NOT NULL UNIQUE CHECK(diskID > 0),\
                                    company TEXT NOT NULL,\
                                    speed INTEGER NOT NULL CHECK(speed > 0),\
                                    free_space INTEGER NOT NULL CHECK(free_space >= 0),\
                                    cost INTEGER NOT NULL CHECK(cost > 0));\
                \
                CREATE TABLE DR(diskID INTEGER NOT NULL CHECK(diskID > 0),\
                                ramID INTEGER NOT NULL CHECK(ramID > 0), \
                                FOREIGN KEY (diskID) REFERENCES TDisk(diskID) ON DELETE CASCADE, \
                                FOREIGN KEY (ramID) REFERENCES TRAM(ramID) ON DELETE CASCADE, \
                                PRIMARY KEY (diskID, ramID), UNIQUE(diskID, ramID));\
                \
                CREATE TABLE DQ(diskID INTEGER NOT NULL CHECK(diskID > 0),\
                                queryID INTEGER NOT NULL CHECK(queryID > 0), \
                                FOREIGN KEY (diskID) REFERENCES TDisk(diskID) ON DELETE CASCADE, \
                                FOREIGN KEY (queryID) REFERENCES TQuery(queryID) ON DELETE CASCADE, \
                                PRIMARY KEY (diskID, queryID), UNIQUE(diskID, queryID));")





# TODO = add table that maps Quries to the disks they are on (the relation is "stored")


def clearTables():  # TODO - check this SQL code ↓ and also reduce to a single query (maybe using group?)
    sql_command("DELETE FROM TQuery;\
                DELETE FROM TRAM;\
                DELETE FROM TDisk")


def dropTables():  # TODO - check this SQL code ↓ and also reduce to a single query (maybe using grouping?)
    sql_command("DROP TABLE IF EXISTS TQuery CASCADE;\
                DROP TABLE IF EXISTS TRAM CASCADE;\
                DROP TABLE IF EXISTS TDisk CASCADE;\
                DROP TABLE IF EXISTS DR CASCADE;\
                DROP TABLE IF EXISTS DQ CASCADE;")


def addQuery(query: Query) -> ReturnValue:
    return insert("TQuery", query)


def getQueryProfile(queryID: int) -> Query:
    ret = sql_command("SELECT * FROM TQuery WHERE queryID = {}".format(queryID))
    if ret.result is not None:
        return ret.result
    else:
        return Query.badQuery()


def deleteQuery(query: Query) -> ReturnValue:
    # TODO - do not forget to adjust the free space on disk if the query runs on one. Hint - think about transactions in such cases (there are more in this assignment).
    ret = sql_command(f"BEGIN; \
                        UPDATE TDisk \
                        SET free_space = free_space + {query.getSize()} \
                        WHERE diskID IN (SELECT diskID FROM DQ WHERE queryID = {query.getQueryID()}); \
                        DELETE FROM TQuery WHERE queryID = {query.getQueryID()}; \
                        COMMIT;", to_commit=False)
    return ret.ret_val


def addDisk(disk: Disk) -> ReturnValue:
    return insert("TDisk", disk)


def getDiskProfile(diskID: int) -> Disk:
    ret = sql_command("SELECT * FROM TDisk WHERE diskID = {}".format(diskID))
    if ret.result is not None:
        return ret.result
    else:
        return Disk.badDisk()


def deleteDisk(diskID: int) -> ReturnValue:
    ret = sql_command(f"DELETE FROM TDisk WHERE diskID = {diskID}") #;\
                        # DELETE FROM DR WHERE diskID = {diskID};\
                        # DELETE FROM DQ WHERE diskID = {diskID};")
    if ret.rows_affected == 0:
        return ReturnValue.NOT_EXISTS
    return ret.ret_val


def addRAM(ram: RAM) -> ReturnValue:
    return insert("TRAM", ram)


def getRAMProfile(ramID: int) -> RAM:
    ret = sql_command("SELECT * FROM TRAM WHERE TRAM.ramID = {}".format(ramID))
    if ret.result is not None:
        return ret.result
    else:
        return RAM.badRAM()


def deleteRAM(ramID: int) -> ReturnValue:
    ret = sql_command(f"DELETE FROM TRAM WHERE TRAM.ramID = {ramID}") #; \
                        #DELETE FROM DR WHERE ramID = {ramID};")
    if ret.rows_affected == 0:
        return ReturnValue.NOT_EXISTS
    return ret.ret_val


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    # TODO - what about rollback?
    return sql_command(f"BEGIN; \
                        INSERT INTO TQuery values ({str(query.__dict__.values())[13:-2]});\
                        INSERT INTO TDisk values ({str(disk.__dict__.values())[13:-2]});\
                        COMMIT;", to_commit=False).ret_val

    # return sql_command("BEGIN; \
    #     INSERT INTO TQuery values (" + str(query.__dict__.values())[13:-2] + ");" + \
    #     "INSERT INTO TDisk values (" + str(disk.__dict__.values())[13:-2] + ");" + \
    #     "COMMIT;", to_commit=False).ret_val  # TODO double commit??


class pair:
    def __init__(self, x=None, y=None):
        self.__x = x
        self.__y = y


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:

    obj = pair(diskID, query.getQueryID())
    return sql_command("BEGIN; \
                        INSERT INTO DQ values ({}); \
                        UPDATE TDisk \
                        SET free_space = free_space - {} \
                        WHERE diskID = {}; \
                        COMMIT;".format(str((obj.__dict__.values()))[13:-2], query.getSize(), diskID), to_commit=False).ret_val


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:

    return sql_command(f"BEGIN; \
                        DELETE FROM DQ WHERE queryID = {query.getQueryID()} AND diskID = {diskID}; \
                        UPDATE TDisk \
                        SET free_space = free_space + {query.getSize()} \
                        WHERE diskID = {diskID}; \
                        COMMIT;", to_commit=False).ret_val


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return sql_command("INSERT INTO DR values ({}, {})".format(diskID, ramID)).ret_val
    # return sql_command("INSERT INTO DR values (" + str(diskID) + ", " + str(ramID) + ")").ret_val


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return sql_command("DELETE FROM DR WHERE diskID = {} AND ramID = {}".format(diskID, ramID))
    # TODO ReturnValue, does not return NOT_EXISTS if RAM/disk does not exist or RAM is not a part of disk


def averageSizeQueriesOnDisk(diskID: int) -> float: # checked - GOOD
    ret = sql_command(f"SELECT CAST(AVG(size) AS FLOAT)\
                       FROM DQ  NATURAL JOIN TQuery \
                       WHERE diskID = {diskID}")
    return ret.result


def diskTotalRAM(diskID: int) -> int:
    ret = sql_command("SELECT COALESCE(SUM(size), 0) FROM TRAM \
        WHERE ramID in (SELECT ramID FROM DR WHERE diskID = {})".format(diskID))

    if ret.result is None:  # TODO not sure about it. maybe add another wrapper func for that?
        return 0
    elif ret.ret_val != ReturnValue.OK:
        return -1
    return ret.result


def getCostForPurpose(purpose: str) -> int:
    # ret = sql_command("SELECT * FROM TQuery WHERE purpose <> {}".format("as"))
    ret = sql_command("SELECT * FROM TDisk WHERE TDisk.company = " + str("sdf"))

    # ret = sql_command("SELECT * FROM TQuery \
    #                         WHERE purpose = {}".format("A"))

    # ret = sql_command("SELECT * FROM TQuery \
    #                         WHERE purpose = {}".format("A"))

    # ret = sql_command("SELECT TDisk.cost, TQuery.size, TDisk.cost*TQuery.size AS TmpCalc FROM \
    #                  TDisk INNER JOIN DQ ON TDisk.diskID = DQ.diskID \
    #                 INNER JOIN TQuery ON DQ.queryID = TQuery.queryID \
    #                 WHERE TQuery.purpose = {}".format(purpose))

    # ret = sql_command("SELECT COALESCE(SUM(TmpTable.TmpCalc), 0) AS CostForPurpose FROM \
    #             (SELECT TDisk.cost, TQuery.size, TDisk.cost*TQuery.size AS TmpCalc FROM \
    #              TDisk INNER JOIN DQ ON TDisk.diskID = DQ.diskID \
    #             INNER JOIN TQuery ON TQuery.queryID = DQ.queryID \
    #             WHERE TQuery.purpose = {}) AS TmpTable".format(purpose))

    if ret.result is None:  # TODO not sure about it. maybe add another wrapper func for that?
        return 0
    elif ret.ret_val != ReturnValue.OK:
        return -1
    return ret.result
    #
    # ret = sql_command("SELECT CASE WHEN NOT EXISTS(SELECT * FROM TDisk WHERE diskID = {}) OR \
    #                         EXISTS(SELECT * FROM TDisk INNER JOIN DR ON TDisk.diskID = DR.diskID \
    #                         INNER JOIN TRAM ON DR.ramID = TRAM.ramID \
    #                         WHERE TDisk.company <> TRAM.company AND TDisk.diskID = {}) THEN CAST(0 AS BIT) \
    #                         ELSE CAST(1 AS BIT) END".format(diskID, diskID))


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    ret = sql_command(f"SELECT queryID\
                        FROM TQuery, (SELECT free_space FROM TDisk WHERE diskID = {diskID}) as Temp\
                        WHERE size <= free_space\
                        LIMIT 5")
    return ret.result  # TODO - return a list!


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    ret = sql_command(f"SELECT queryID\
                            FROM TQuery, (SELECT free_space FROM TDisk WHERE diskID = {diskID}) as Temp\
                            WHERE size <= free_space AND size <= (SELECT SUM(size) FROM DR NATURAL JOIN TRAM WHERE diskID = {diskID})\
                            LIMIT 5")
    return ret.result  # TODO - return a list!


def isCompanyExclusive(diskID: int) -> bool:
    # ret = sql_command("SELECT CASE WHEN NOT EXISTS(SELECT * FROM TDisk WHERE diskID = {}) OR \
    #                     EXISTS(SELECT * FROM TDisk INNER JOIN (SELECT * FROM DR WHERE DR.diskID = {}}) DR_tmp ON TDisk.diskID = DR_tmp.diskID \
    #                     INNER JOIN TRAM ON DR_tmp.ramID = TRAM.ramID \
    #                     WHERE TDisk.company <> TRAM.company AND TDisk.diskID = {}) THEN CAST(0 AS BIT) \
    #                     ELSE CAST(1 AS BIT) END".format(diskID, diskID, diskID))
    ret = sql_command("SELECT CASE WHEN NOT EXISTS(SELECT * FROM TDisk WHERE diskID = {}) OR \
                        EXISTS(SELECT * FROM TDisk INNER JOIN DR ON TDisk.diskID = DR.diskID \
                        INNER JOIN TRAM ON DR.ramID = TRAM.ramID \
                        WHERE TDisk.company <> TRAM.company AND TDisk.diskID = {}) THEN CAST(0 AS BIT) \
                        ELSE CAST(1 AS BIT) END".format(diskID, diskID))
    if ret.ret_val != ReturnValue.OK:
        return False
    return '1' in str(ret.result)  # TODO like that??


def getConflictingDisks() -> List[int]:
    ret = sql_command("SELECT DISTINCT A.diskID FROM DQ A, DQ B \
                      WHERE A.queryID = B.queryID AND A.diskID <> B.diskID \
                      ORDER BY A.diskID LIMIT 5")
    return ret.result

def mostAvailableDisks() -> List[int]:
    sql_command("CREATE VIEW DiskQRunnable AS \
                    SELECT diskID, speed, queryID \
                    FROM TQuery, TDisk \
                    WHERE size <= free_space \
                    ORDER BY diskID")

    ret = sql_command("SELECT diskID \
                        FROM (  SELECT diskID, COUNT(diskID), speed\
                                FROM DiskQRunnable\
                                GROUP BY (diskID, speed) \
                                ORDER BY count DESC, speed DESC, diskID ASC ) AS Sub\
                        LIMIT 5")

    sql_command("DROP VIEW DiskQRunnable")

    return ret.result  # TODO - return a list!
    # return []


def getCloseQueries(queryID: int) -> List[int]:  # TODO
    # ret = sql_command("SELECT DISTINCT A.diskID FROM DQ A, DQ B \
    #                       WHERE A.queryID = B.queryID AND A.diskID <> B.diskID \
    #                       ORDER BY A.diskID LIMIT 5")
    # return ret.result
    return 0




def test_getCostForPurpose():
    for i in range(1, 10):
        addDisk(Disk(i, "company_" + str(i), 10 * i, 100 * i, 1000 * i))
    # for i in range(1,10):
    #     addQuery(Query(i, "A", i))

    q1 = Query(1, "Aaa", 1)
    addQuery(q1)
    q2 = Query(2, "Aaa", 20)
    addQuery(q2)
    q3 = Query(3, "Aaa", 300)
    addQuery(q3)
    q4 = Query(4, "B", 4000)
    addQuery(q4)
    q5 = Query(5, "B", 50000)
    addQuery(q5)

    addQueryToDisk(q1, 1)
    addQueryToDisk(q2, 1)
    addQueryToDisk(q3, 1)
    print(getCostForPurpose("A"))


def test_getConflictingDisks():
    for i in range(1, 5):
        addDisk(Disk(i, "company_" + str(i), 10 * i, 100 * i, 1000 * i))
        q1 = Query(i, "Aaa", 1)
        addQuery(q1)
        addQueryToDisk(q1, i)

    assert(str(getConflictingDisks()) == "\n")

    q1 = Query(10, "Aaa", 1)
    addQuery(q1)
    for i in range(1, 10):
        addDisk(Disk(10 + i, "company_" + str(i), 10 * i, 100 * i, 1000 * i))
        addQueryToDisk(q1, 10 + i)

    # print(str(getConflictingDisks()))
    assert(str(getConflictingDisks()).replace(' ', '').replace('\n', '') == 'diskid1112131415')

    removeQueryFromDisk(q1, 13)
    removeQueryFromDisk(q1, 14)
    removeQueryFromDisk(q1, 15)
    # print(str(getConflictingDisks()))
    assert (str(getConflictingDisks()).replace(' ', '').replace('\n', '') == 'diskid1112161718')

    q1 = Query(20, "Bbb", 1)
    addQuery(q1)
    for i in range(1, 10, 2):
        addQueryToDisk(q1, 10 + i)

    # print(str(getConflictingDisks()))
    assert (str(getConflictingDisks()).replace(' ', '').replace('\n', '') == 'diskid1112131516')
    print("@@@ PASS test_getConflictingDisks @@@" + " however, wait for more tests...")


def test_isCompanyExclusive():
    for i in range(1, 10):
        addDisk(Disk(i, "company_"+str(i), 10*i, 100*i, 1000*i))
        # print(getDiskProfile(i))
        assert(isCompanyExclusive(i) is True)
        addRAM(RAM(i, "company_" + str(i), i * i))
        addRAMToDisk(i, i)

        assert(isCompanyExclusive(i) is True)

    assert(isCompanyExclusive(9999) is False)
    assert(isCompanyExclusive(-999) is False)

    for i in range(1, 10):
        addRAM(RAM(10+i, "company_"+str(i), i*i))
        addRAMToDisk(10+i, 1)
    # print(isCompanyExclusive(1))
    assert(isCompanyExclusive(1) is False)

    for i in range(1, 10):
        addRAM(RAM(100+i, "company_2", i*i))
        addRAMToDisk(100+i, 2)
    # print(isCompanyExclusive(2))
    assert(isCompanyExclusive(2) is True)
    addRAM(RAM(666, "sdjfbskdf", 66666))
    addRAMToDisk(666, 2)
    assert(isCompanyExclusive(2) is False)
    removeRAMFromDisk(666, 2)
    assert(isCompanyExclusive(2) is True)
    addRAMToDisk(666, 2)
    assert(isCompanyExclusive(2) is False)
    deleteRAM(666)
    assert(isCompanyExclusive(2) is True)

    print("@@@ PASS test_isCompanyExclusive @@@")

def test_deleteQuery():
    addDisk(Disk(diskID=10, company="z", speed=100, free_space=7654321, cost=5))
    q1 = Query(1, "test1", 1)
    q2 = Query(2, "test2", 20)
    q3 = Query(3, "test2", 300)
    q4 = Query(4, "test4", 4000)
    q5 = Query(5, "test5", 50000)
    addQuery(q1)
    addQuery(q2)
    addQuery(q3)
    addQuery(q4)
    addQuery(q5)
    addQueryToDisk(q1, 10)
    addQueryToDisk(q2, 10)
    addQueryToDisk(q3, 10)
    deleteQuery(q3)
    assert("7654300" in str(getDiskProfile(10)))
    deleteQuery(q1)
    assert("7654301" in str(getDiskProfile(10)))
    deleteQuery(q2)
    assert("7654321" in str(getDiskProfile(10)))

    addQueryToDisk(q4, 10)
    assert("7650321" in str(getDiskProfile(10)))
    addQueryToDisk(q5, 10)
    assert("7600321" in str(getDiskProfile(10)))
    deleteQuery(q4)
    assert("7604321" in str(getDiskProfile(10)))
    deleteQuery(q5)
    assert("7654321" in str(getDiskProfile(10)))
    print("@@@ PASS test_deleteQuery @@@")


def test_avg_q_size_on_disk():
    addDisk(Disk(diskID=10, company="z", speed=100, free_space=80, cost=5))
    q1 = Query(1, "test1", 3)
    q2 = Query(2, "test2", 4)
    q3 = Query(3, "test2", 50)
    q4 = Query(5, "test4", 10)

    addQuery(q1)
    addQuery(q2)
    addQuery(q3)

    addQueryToDisk(q1, 10)
    addQueryToDisk(q2, 10)
    addQueryToDisk(q3, 10)

    x = averageSizeQueriesOnDisk(10)
    assert("19" in str(x))
    deleteQuery(q2)
    x = averageSizeQueriesOnDisk(10)
    assert("26.5" in str(x))
    addQuery(q4)
    addQueryToDisk(q4, 10)
    x = averageSizeQueriesOnDisk(10)
    assert("21" in str(x))
    # removeQueryFromDisk(q2, 10)  # nothing happens
    x = averageSizeQueriesOnDisk(10)
    assert("21" in str(x))

    removeQueryFromDisk(q1, 10)
    x = averageSizeQueriesOnDisk(10)
    assert("30" in str(x))

    print("test_avg_q_size_on_disk - SUCCESS")

def can_be_added_ram_test():

    d1 = Disk(diskID=7, company="z", speed=100, free_space=80, cost=5)
    d2 = Disk(diskID=13, company="z", speed=100, free_space=40, cost=5)
    d3 = Disk(diskID=1, company="z", speed=100, free_space=2, cost=5)
    d4 = Disk(diskID=2, company="z", speed=100, free_space=3, cost=5)
    d5 = Disk(diskID=3, company="z", speed=100, free_space=4, cost=5)
    d6 = Disk(diskID=4, company="z", speed=100, free_space=11, cost=5)
    d7 = Disk(diskID=5, company="z", speed=90, free_space=51, cost=5)

    addDisk(d1)
    addDisk(d2)
    addDisk(d3)
    addDisk(d4)
    addDisk(d5)
    addDisk(d6)
    addDisk(d7)


    q1 = Query(1, "test1", 3)
    q2 = Query(2, "test2", 4)
    q3 = Query(3, "test2", 50)
    q4 = Query(4, "test4", 10)
    addQuery(q1)
    addQuery(q2)
    addQuery(q3)
    addQuery(q4)

    r1 = RAM(ramID=1, company='z', size=3)
    r2 = RAM(ramID=2, company='t', size=30)
    r3 = RAM(ramID=3, company='k', size=50)
    addRAM(r1)
    addRAM(r2)
    addRAM(r3)

    print(getQueriesCanBeAddedToDiskAndRAM(7))
    print(getQueriesCanBeAddedToDiskAndRAM(13))

    print("after 1st")

    addRAMToDisk(1,7)
    addRAMToDisk(1,13)
    print(getQueriesCanBeAddedToDiskAndRAM(7))
    print(getQueriesCanBeAddedToDiskAndRAM(13))

    print("after 2nd")
    addRAMToDisk(3,7)
    addRAMToDisk(2,13)
    print(getQueriesCanBeAddedToDiskAndRAM(7))
    print(getQueriesCanBeAddedToDiskAndRAM(13))

    print("after 3rd")




if __name__ == '__main__':
    dropTables()
    createTables()
    # test_isCompanyExclusive()
    # test_avg_q_size_on_disk()
    # test_deleteQuery()
    can_be_added_ram_test()

