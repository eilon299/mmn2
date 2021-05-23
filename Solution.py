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
        print(e)
        return SQLRet(ReturnValue.ERROR)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return SQLRet(ReturnValue.BAD_PARAMS)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return SQLRet(ReturnValue.BAD_PARAMS)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return SQLRet(ReturnValue.ALREADY_EXISTS)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
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
                DROP TABLE IF EXISTS DQ CASCADE")


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
                        WHERE (diskID,{query.getQueryID()}) IN DQ; \
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
    return ret.ret_val


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:

    # TODO - what about rollback?
    return sql_command(f"BEGIN; \
                        INSERT INTO TQuery values ({str(query.__dict__.values())[13:-2]});\
                        INSERT INTO TDisk values ({str(disk.__dict__.values())[13:-2]});\
                        COMMIT;", to_commit=False).ret_val  # TODO double commit??

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
    return sql_command("SELECT AVG(size) as Average_Size_Queries_On_Disk\
                       FROM DQ NATURAL JOIN TQuery \
                       WHERE diskID = diskID").result


def diskTotalRAM(diskID: int) -> int:
    ret = sql_command("SELECT COALESCE(SUM(size), 0) FROM TRAM \
        WHERE ramID in (SELECT ramID FROM DR WHERE diskID = {})".format(diskID))

    if ret.result is None:  # TODO not sure about it. maybe add another wrapper func for that?
        return 0
    elif ret.ret_val != ReturnValue.OK:
        return -1
    return ret.result


def getCostForPurpose(purpose: str) -> int:
    return 0


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    ret = sql_command("SELECT CASE WHEN NOT EXISTS(SELECT * FROM TDisk WHERE diskID = {}) OR \
                        EXISTS(SELECT * FROM TDisk INNER JOIN DR ON TDisk.diskID = DR.diskID \
                        INNER JOIN TRAM ON DR.ramID = TRAM.ramID \
                        WHERE TDisk.company <> TRAM.company AND TDisk.diskID = {}) THEN CAST(0 AS BIT) \
                        ELSE CAST(1 AS BIT) END".format(diskID, diskID))
    # ret = sql_command("SELECT CASE WHEN NOT EXISTS(SELECT * FROM TDisk WHERE diskID = {}) THEN CAST(0 AS BIT) \
    #                     ELSE CAST(1 AS BIT) END".format(diskID))
    if ret.ret_val != ReturnValue.OK:
        return False
    return '1' in str(ret.result)  # TODO like that??


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseQueries(queryID: int) -> List[int]:
    return []






def test_isCompanyExclusive():
    for i in range(1, 10):
        addDisk(Disk(i, "company_"+str(i), 10*i, 100*i, 1000*i))
        # print(getDiskProfile(i))
        # print(isCompanyExclusive(i))
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
    assert("19.00000000" in str(x))
    deleteQuery(q2)
    x = averageSizeQueriesOnDisk(10)
    assert("26.50" in str(x))

    addQuery(q4)
    addQueryToDisk(q4, 10)
    x = averageSizeQueriesOnDisk(10)
    assert("21.00" in str(x))
    # removeQueryFromDisk(q2, 10)  # nothing happens
    x = averageSizeQueriesOnDisk(10)
    assert("21.00" in str(x))

    removeQueryFromDisk(q1, 10)
    x = averageSizeQueriesOnDisk(10)
    assert("30.00" in str(x))

    print("test_avg_q_size_on_disk - SUCCESS")




if __name__ == '__main__':
    dropTables()
    createTables()
    # test_isCompanyExclusive()
    test_avg_q_size_on_disk()



    # # q = Query(1, "test", 5)
    # # addQuery(Query(1, "test", 1 * 5))
    #
    # for i in range(1, 4):
    #     addQuery(Query(i, "test", i*i))
    # deleteQuery(Query(2, "test", 1))
    #
    # for i in range(1, 4):
    #     addDisk(Disk(i, "Eil", 100, 1, 100000))
    # deleteDisk(2)
    #
    # for i in range(1, 4):
    #     addRAM(RAM(i, "sdfsd", 100*i))
    # deleteRAM(2)
    #
    # print(getDiskProfile(1))
    # print(getQueryProfile(1))
    # print(getRAMProfile(1))
    # addQuery(Query(77, "test", 1 * 5))
    # q1 = Query(77, "test", 1 * 5)


    # print(str((q1.__dict__.values()))[13:-2])
    # print(str((q1.__dict__.values())))
    #
    # r1 = RAM(12, "sdfsd", 100*12)
    # print(str((r1.__dict__.values()))[13:-2])
    # print(str((r1.__dict__.values())))
    #
    # d1 = Disk(234, "Eil", 100, 1, 100000)
    # print(str((d1.__dict__.values()))[13:-2])
    # print(str((d1.__dict__.values())))

    # addRAMToDisk(1,2)
    # addDisk(Disk(1, "2122ed21", 1122, 2112, 212))
    # addDisk(Disk(2, "2122ed21", 1122, 2112, 212))
    # addDisk(Disk(3, "2122ed21", 1122, 2112, 212))
    # addDisk(Disk(4, "2122ed21", 1122, 2112, 212))
    # addRAM(RAM(10, "ram_comp", 1010))
    # addRAM(RAM(20, "ram_comp", 1010))
    # addRAM(RAM(30, "ram_comp", 1010))
    #
    # addRAMToDisk(10,1)
    # addRAMToDisk(20,1)
    # addRAMToDisk(30,1)
    #
    # addRAMToDisk(10, 2)
    # addRAMToDisk(20, 2)
    # addRAMToDisk(30, 2)

    # # deleteRAM(30)
    # # deleteDisk(1)
    # removeRAMFromDisk(10, 2)

    # addDisk(Disk(333, "AAA", 3, 33, 333))
    # addRAM(RAM(7001, "ram_comp", 700))
    # addRAM(RAM(701, "ram_comp", 70))
    # addRAM(RAM(71, "ram_comp", 7))
    # addRAMToDisk(71, 333)
    # addRAMToDisk(701, 333)
    # addRAMToDisk(7001, 333)
    # print(diskTotalRAM(333))
    # removeRAMFromDisk(701, 333)
    # removeRAMFromDisk(701, 333)
    # print(diskTotalRAM(333))
    # print(diskTotalRAM(3))
    # isCompanyExclusive(3)
    #
    #
    # addDisk(Disk(1234, "2122ed21", 1122, 2112, 212))


    # addDiskAndQuery(Disk(55, "fivefive", 555, 5555, 55555), Query(77, "seven", 7777))

