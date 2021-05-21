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
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute(query, printSchema)
        if to_commit:
            conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.ERROR  # TODO - notice if this is correct
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
                cost INTEGER NOT NULL CHECK(cost > 0))")


# TODO = add table that maps Quries to the disks they are on (the relation is "stored")


def clearTables():  # TODO - check this SQL code ↓ and also reduce to a single query (maybe using group?)
    sql_command("DELETE FROM TQuery;\
                DELETE FROM TRAM;\
                DELETE FROM TDisk")


def dropTables():  # TODO - check this SQL code ↓ and also reduce to a single query (maybe using grouping?)
    sql_command("DROP TABLE IF EXISTS TQuery CASCADE;\
                DROP TABLE IF EXISTS TRAM CASCADE;\
                DROP TABLE IF EXISTS TDisk CASCADE")


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
    ret = sql_command("DELETE FROM TQuery WHERE queryID = {}".format(query.getQueryID()))
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
    ret = sql_command("DELETE FROM TDisk WHERE diskID = {}".format(diskID))
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
    ret = sql_command("DELETE FROM TRAM WHERE TRAM.ramID = {}".format(ramID))
    return ret.ret_val


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    # return ReturnValue.OK
    # TODO - need in 1 Q to assure both actions will succeed and also do them -> Transaction - recitation 7
    return sql_command("BEGIN; \
        INSERT INTO TQuery values (" + str(query.__dict__.values())[13:-2] + ");" + \
        "INSERT INTO TDisk values (" + str(disk.__dict__.values())[13:-2] + ");" + \
        "COMMIT;", to_commit=False).ret_val  # TODO double commit??

    # return sql_command("INSERT INTO {} values ({})".format(table, str((obj.__dict__.values()))[13:-2])).ret_val



def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averageSizeQueriesOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForPurpose(purpose: str) -> int:
    return 0


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseQueries(queryID: int) -> List[int]:
    return []



if __name__ == '__main__':
    dropTables()
    createTables()
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
    addDiskAndQuery(Disk(55, "fivefive", 555, 5555, 55555), Query(77, "seven", 7777))



