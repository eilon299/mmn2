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
        # print(e)
        return SQLRet(ReturnValue.ERROR)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        if not to_commit:
            conn.rollback()
        # print(e)
        return SQLRet(ReturnValue.BAD_PARAMS)
    except DatabaseException.CHECK_VIOLATION as e:
        if not to_commit:
            conn.rollback()
        # print(e)
        return SQLRet(ReturnValue.BAD_PARAMS)
    except DatabaseException.UNIQUE_VIOLATION as e:
        if not to_commit:
            conn.rollback()
        # print(e)
        return SQLRet(ReturnValue.ALREADY_EXISTS)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        if not to_commit:
            conn.rollback()
        # print(e)
        return SQLRet(ReturnValue.NOT_EXISTS)
    except Exception as e:
        if not to_commit:
            conn.rollback()
        # print(e)
        return SQLRet(ReturnValue.ERROR)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return SQLRet(ReturnValue.OK, rows_effected, result)


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
                                PRIMARY KEY (diskID, queryID), UNIQUE(diskID, queryID)); \
                \
                CREATE VIEW DCanRunQ AS \
                                    SELECT tmp.diskID, TQuery.queryID, TQuery.size \
                                    FROM TQuery, (SELECT free_space, diskID FROM TDisk) AS tmp \
                                    WHERE TQuery.size <= tmp.free_space; \
                            \
                CREATE VIEW D_total_RAM AS \
                        SELECT diskID , COALESCE((  SELECT SUM(size) \
                                                        FROM TRAM \
                                                        WHERE TRAM.ramID in (SELECT ramID FROM DR AS Dtemp WHERE Dtemp.diskID = TDisk.diskID) \
                                                     ), 0) AS totalRam \
                        FROM TDisk")


def clearTables():
    sql_command("DELETE FROM TQuery;\
                DELETE FROM TRAM;\
                DELETE FROM TDisk")


def dropTables():
    sql_command("DROP TABLE IF EXISTS TQuery CASCADE;\
                DROP TABLE IF EXISTS TRAM CASCADE;\
                DROP TABLE IF EXISTS TDisk CASCADE;\
                DROP TABLE IF EXISTS DR CASCADE;\
                DROP TABLE IF EXISTS DQ CASCADE;\
                DROP TABLE IF EXISTS DCanRunQ;\
                DROP TABLE IF EXISTS D_total_RAM;")

def addQuery(query: Query) -> ReturnValue:
    q = sql.SQL("INSERT INTO TQuery(queryID, purpose, size) VALUES({i}, {p}, {s})").format(
                                                                                    i=sql.Literal(query.getQueryID()),
                                                                                    p=sql.Literal(query.getPurpose()),
                                                                                    s=sql.Literal(query.getSize()))
    return sql_command(q).ret_val


def getQueryProfile(queryID: int) -> Query:
    ret = sql_command(sql.SQL("SELECT * FROM TQuery WHERE queryID = {}").format(sql.Literal(queryID)))
    if ret.rows_affected != 0 and ret.ret_val == ReturnValue.OK:
        return Query(ret.result.rows[0][0], ret.result.rows[0][1], ret.result.rows[0][2])
    else:
        return Query.badQuery()


def deleteQuery(query: Query) -> ReturnValue:
    q = sql.SQL("BEGIN; \
                UPDATE TDisk \
                SET free_space = free_space + {size} \
                WHERE diskID IN (SELECT diskID FROM DQ WHERE queryID = {id}); \
                DELETE FROM TQuery WHERE queryID = {id}; \
                COMMIT;").format(size=sql.Literal(query.getSize()),
                                 id=sql.Literal(query.getQueryID()))
    ret = sql_command(q, to_commit=False)
    return ret.ret_val


def addDisk(disk: Disk) -> ReturnValue:
    q = sql.SQL("INSERT INTO TDisk(diskID, company, speed, free_space, cost) VALUES({i}, {comp}, {sp}, {fs}, {cost})")\
        .format(i=sql.Literal(disk.getDiskID()),
                comp=sql.Literal(disk.getCompany()),
                sp=sql.Literal(disk.getSpeed()),
                fs=sql.Literal(disk.getFreeSpace()),
                cost=sql.Literal(disk.getCost()))
    return sql_command(q).ret_val


def getDiskProfile(diskID: int) -> Disk:
    ret = sql_command("SELECT * FROM TDisk WHERE diskID = {}".format(diskID))
    if ret.rows_affected != 0 and ret.ret_val == ReturnValue.OK:
        return Disk(ret.result.rows[0][0], ret.result.rows[0][1], ret.result.rows[0][2], ret.result.rows[0][3], ret.result.rows[0][4])
    else:
        return Disk.badDisk()


def deleteDisk(diskID: int) -> ReturnValue:
    ret = sql_command(f"DELETE FROM TDisk WHERE diskID = {diskID}")
    if ret.rows_affected == 0:
        return ReturnValue.NOT_EXISTS
    return ret.ret_val


def addRAM(ram: RAM) -> ReturnValue:
    q = sql.SQL("INSERT INTO TRam(ramID, company, size) VALUES({id}, {comp}, {size})").format(
                                                                                    id=sql.Literal(ram.getRamID()),
                                                                                    comp=sql.Literal(ram.getCompany()),
                                                                                    size=sql.Literal(ram.getSize()))
    return sql_command(q).ret_val


def getRAMProfile(ramID: int) -> RAM:
    ret = sql_command("SELECT * FROM TRAM WHERE TRAM.ramID = {}".format(ramID))
    if ret.rows_affected != 0 and ret.ret_val == ReturnValue.OK:
        return RAM(ret.result.rows[0][0], ret.result.rows[0][1], ret.result.rows[0][2])
    else:
        return RAM.badRAM()


def deleteRAM(ramID: int) -> ReturnValue:
    ret = sql_command(f"DELETE FROM TRAM WHERE TRAM.ramID = {ramID}")
    if ret.rows_affected == 0:
        return ReturnValue.NOT_EXISTS
    return ret.ret_val


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    q = sql.SQL("BEGIN; \
                INSERT INTO TQuery values ({q_id}, {q_purpose}, {q_size});\
                INSERT INTO TDisk values ({d_id}, {d_comp}, {d_speed}, {d_fs}, {d_cost});\
                COMMIT;").format(q_id=sql.Literal(query.getQueryID()),
                                 q_purpose=sql.Literal(query.getPurpose()),
                                 q_size=sql.Literal(query.getSize()),
                                 d_id=sql.Literal(disk.getDiskID()),
                                 d_comp=sql.Literal(disk.getCompany()),
                                 d_speed=sql.Literal(disk.getSpeed()),
                                 d_fs=sql.Literal(disk.getFreeSpace()),
                                 d_cost=sql.Literal(disk.getCost()))
    return sql_command(q, to_commit=False).ret_val


class pair:
    def __init__(self, x=None, y=None):
        self.__x = x
        self.__y = y


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    q = sql.SQL("BEGIN; \
                INSERT INTO DQ values ({d_id}, {q_id}); \
                UPDATE TDisk \
                SET free_space = free_space - {size} \
                WHERE diskID = {d_id}; \
                COMMIT;").format(d_id=sql.Literal(diskID),
                                 q_id=sql.Literal(query.getQueryID()),
                                 size=sql.Literal(query.getSize()))
    return sql_command(q, to_commit=False).ret_val


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    q = sql.SQL("BEGIN; \
                UPDATE TDisk \
                SET free_space = free_space + {size} \
                WHERE diskID = {d_id} AND EXISTS(SELECT * from DQ WHERE queryID = {q_id} AND diskID = {d_id}); \
                DELETE FROM DQ WHERE queryID = {q_id} AND diskID = {d_id}; \
                COMMIT;").format(q_id=sql.Literal(query.getQueryID()),
                                 size=sql.Literal(query.getSize()),
                                 d_id=sql.Literal(diskID))
    return sql_command(q, to_commit=False).ret_val


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return sql_command("INSERT INTO DR values ({}, {})".format(diskID, ramID)).ret_val


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    ret = sql_command("DELETE FROM DR WHERE diskID = {} AND ramID = {}".format(diskID, ramID))
    if ret.rows_affected == 0:
        return ReturnValue.NOT_EXISTS
    return ret.ret_val


def averageSizeQueriesOnDisk(diskID: int) -> float:
    ret = sql_command(f"SELECT CAST(AVG(COALESCE(size)) AS FLOAT)\
                       FROM DQ  NATURAL JOIN TQuery \
                       WHERE diskID = {diskID}")
    if ret.ret_val != ReturnValue.OK:
        return -1
    elif ret.result.rows[0][0] is None:
        return 0
    return ret.result.rows[0][0]


def diskTotalRAM(diskID: int) -> int:
    ret = sql_command("SELECT COALESCE(SUM(totalRam), 0) \
                            FROM D_total_RAM \
                            WHERE diskID = {}".format(diskID))

    if ret.ret_val != ReturnValue.OK:
        return -1
    return ret.result.rows[0][0]


def getCostForPurpose(purpose: str) -> int:
    ret = sql_command(sql.SQL("SELECT COALESCE(SUM(TmpTable.TmpCalc), 0)\
                                FROM  (SELECT TDisk.cost, TQuery.size, TDisk.cost*TQuery.size AS TmpCalc\
                                        FROM TDisk INNER JOIN DQ ON TDisk.diskID = DQ.diskID INNER JOIN TQuery ON TQuery.queryID = DQ.queryID \
                                        WHERE TQuery.purpose = {p_t}) AS TmpTable").format(p_t=sql.Literal(purpose)))

    if ret.ret_val != ReturnValue.OK:
        return -1
    return ret.result.rows[0][0]


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    ret = sql_command(f"SELECT queryID\
                        FROM DCanRunQ\
                        WHERE diskID = {diskID} \
                        ORDER BY queryID DESC \
                        LIMIT 5")

    if ret.ret_val == ReturnValue.OK:
        return [x[0] for x in ret.result.rows]
    else:
        return []

def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    ret = sql_command(f"SELECT queryID\
                        FROM DCanRunQ \
                        WHERE DCanRunQ.diskID = {diskID} AND DCanRunQ.size <= (SELECT totalRam FROM D_total_RAM WHERE D_total_RAM.diskID = {diskID}) \
                        ORDER BY queryID ASC\
                        LIMIT 5")

    if ret.ret_val == ReturnValue.OK:
        return [x[0] for x in ret.result.rows]
    else:
        return []

def isCompanyExclusive(diskID: int) -> bool:
    ret = sql_command("SELECT EXISTS(SELECT * FROM TDisk WHERE diskID = {id}) AND \
                            NOT EXISTS(SELECT * \
                                        FROM TDisk INNER JOIN DR ON TDisk.diskID = DR.diskID \
                                                    INNER JOIN TRAM ON DR.ramID = TRAM.ramID \
                                        WHERE TDisk.company <> TRAM.company AND TDisk.diskID = {id})".format(id=diskID))

    if ret.ret_val != ReturnValue.OK:
        return False
    else:
        return ret.result.rows[0][0]


def getConflictingDisks() -> List[int]:
    ret = sql_command("SELECT DISTINCT A.diskID \
                        FROM DQ A, DQ B \
                        WHERE A.queryID = B.queryID AND A.diskID <> B.diskID \
                        ORDER BY A.diskID ASC \
                        LIMIT 5")
    if ret.ret_val == ReturnValue.OK:
        return [x[0] for x in ret.result.rows]
    else:
        return []


def mostAvailableDisks() -> List[int]:

    ret = sql_command("SELECT diskID \
                        FROM (  SELECT diskID, speed, (SELECT COALESCE(COUNT(queryID), 0)\
                                                        FROM DCanRunQ\
                                                        WHERE DCanRunQ.diskID = TDisk.diskID\
                                                       ) AS counti\
                                FROM TDisk\
                              ) AS Temp\
                        ORDER BY counti DESC, speed DESC, diskID ASC\
                        LIMIT 5")

    if ret.ret_val == ReturnValue.OK:
        return [x[0] for x in ret.result.rows]
    else:
        return []


def getCloseQueries(queryID: int) -> List[int]:
    q = sql.SQL("SELECT queryID \
                FROM    (SELECT TQuery.queryID, (  SELECT COALESCE(COUNT(diskID), 0) \
                                                    FROM DQ \
                                                    WHERE DQ.queryID = TQuery.queryID AND \
                                                            DQ.diskID IN (SELECT diskID FROM DQ WHERE queryID = {id}) \
                                                ) AS sharedDisks \
                        FROM TQuery\
                        ) AS tmpT \
                WHERE queryID <> {id} AND sharedDisks >=   (SELECT COUNT(*) \
                                                            FROM DQ \
                                                            WHERE queryID = {id}\
                                                            )*0.5 \
                ORDER BY tmpT.queryID ASC \
                LIMIT 10").format(id=sql.Literal(queryID))
    ret = sql_command(q)

    if ret.ret_val == ReturnValue.OK:
        return [x[0] for x in ret.result.rows]
    else:
        return []


# if __name__ == '__main__':
#     pass
