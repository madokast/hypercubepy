from sqlite import Database, RowC
from typing import Dict, List, Callable, Tuple, Any, Union, TypeVar
import io

T = TypeVar('T')
TableId = int
LocalColumnId = int
GlobalColumnId = int

class TableFilter:
    # 表内过滤，例如 t0.a=10 或者 t0.a=t0.b 或者 t0.a=(t0.b+t0.c)
    def __init__(self, tableId:TableId, columns:List[str], predicate:Callable[[List[Any]], bool], info:str, cubeInfo:'CubeInfo') -> None:
        self.TableId = tableId
        self.columns = columns
        self.ColumnIds = [cubeInfo.TableColumns[tableId].index(c) for c in columns] # 列ID数组
        self.Predicate = predicate
        self.info = info

class Join:
    # 表间 JOIN 谓词
    def __init__(self, leftTableId:TableId, leftColumnNames:List[str], rightTableId:TableId, rightColumnNames:List[str],
                 predicate:Callable[[List[Any]], bool], info:str, cubeInfo:'CubeInfo', isInnerJoin:bool=False) -> None:
        self.LeftTableId = leftTableId
        self.LeftColumnIds = utils.indexes(cubeInfo.TableColumns[leftTableId], leftColumnNames)
        self.RightTableId = rightTableId
        self.RightColumnIds = utils.indexes(cubeInfo.TableColumns[rightTableId], rightColumnNames)
        self.Predicate = predicate
        self.IsInnerJoin = isInnerJoin
        self.info = info

        assert self.LeftTableId < self.RightTableId

        self.LeftGlobalColumnIds = []
        self.rightGlobalColumnIds = []

        for leftColumnName in leftColumnNames:
            for column in cubeInfo.GlobalColumns:
                if column.TableId == leftTableId and column.ColumnName == leftColumnName:
                    self.LeftGlobalColumnIds.append(column.GlobalColumnId)
                    break
        for rightColumnName in rightColumnNames:
            for column in cubeInfo.GlobalColumns:
                if column.TableId == rightTableId and column.ColumnName == rightColumnName:
                    self.rightGlobalColumnIds.append(column.GlobalColumnId)
                    break
        
        assert len(self.LeftGlobalColumnIds) == len(leftColumnNames), f"{self.LeftGlobalColumnIds} {leftColumnNames}"
        assert len(self.rightGlobalColumnIds) == len(rightColumnNames), f"{self.rightGlobalColumnIds} {rightColumnNames}"

class Column:
    # 列信息。保留所属表ID，表内ID，全局ID，列名
    def __init__(self, tableId:TableId, localColumnId:LocalColumnId, globalColumnId:GlobalColumnId, columnName:str) -> None:
        self.TableId = tableId # 表ID
        self.LocalColumnId = localColumnId # 局部列ID
        self.GlobalColumnId = globalColumnId # 全局列ID
        self.ColumnName = columnName
    def __hash__(self) -> int:
        return self.GlobalColumnId
    def __eq__(self, __value: 'Column') -> bool:
        return self.GlobalColumnId == __value.GlobalColumnId
    def __str__(self) -> str:
        return f"t{self.TableId}.{self.ColumnName}"
    def __repr__(self) -> str:
        return str(self)

class CubeInfo:
    def __init__(self) -> None:
        self.Tables:List[str] = [] # 涉及的表，并按照顺序编号 t0/t1/t2
        self.TableColumns:List[List[str]] = [] # 每张表涉及的列，外层为表编号，内层为多个列名，并按顺序编号 c0/c1/c2。称为局部编号
        self.TableFilters:Dict[TableId, List[TableFilter]] = dict() # 表内过滤。例如 t0.a=10 或者 t0.a=t0.b 或者 t0.a=(t0.b+t0.c)
        self.Joins:List[Join] = [] # JOIN 连接过滤，例如 t0.a=t1.a、t0.a>t2.b。满足左侧表的 tableId  

        self.GlobalColumns:List[Column] = [] # 衍生，所有涉及列全局编号。和 CubeKey 一一对应
        self.preprocess()
    
    def setTables(self, *tables:str) -> None:
        self.Tables.extend(tables)
    
    def setColumns(self, tableId:TableId, *columns:str) -> None:
        assert len(self.TableColumns) == tableId, "set columns out of order"
        self.TableColumns.append(list(columns))
        self.preprocess()
    
    def addFilter(self, filter:TableFilter) -> None:
        fs = self.TableFilters.get(filter.TableId, [])
        fs.append(filter)
        self.TableFilters[filter.TableId] = fs

    def addJoin(self, join:Join)-> None:
        self.Joins.append(join)
    
    def preprocess(self) -> None:
        self.GlobalColumns.clear()
        gid = 0
        for tableId in range(len(self.TableColumns)):
            columns = self.TableColumns[tableId]
            for columnId in range(len(columns)):
                self.GlobalColumns.append(Column(tableId, columnId, gid, columns[columnId]))
                gid+=1
    
    def newCubeKey(self) -> 'CubeKey':
        return CubeKey(len(self.GlobalColumns))
    
    def tableNumber(self) -> int:
        return len(self.Tables)
    
    def newRowSetList(self) -> 'CubeValue':
        return CubeValue(self.tableNumber())
    
    def getGlobalColumnIds(self, tableId:TableId, columnNames:List[str]) -> List[GlobalColumnId]:
        r = []
        for columnName in columnNames:
            for gColumn in self.GlobalColumns:
                if gColumn.TableId == tableId and gColumn.ColumnName == columnName:
                    r.append(gColumn.GlobalColumnId)
                    break
        return r
    
    def getJoins(self, tableId:TableId) -> List[Join]:
        r = []
        for join in self.Joins:
            if join.RightTableId == tableId:
                r.append(join)
        return r
    
    def __str__(self) -> str:
        sb = io.StringIO() # string-builder
        for tableId, tableName in zip(range(1024), self.Tables):
            sb.write(f"TABLE {tableName} AS t{tableId}\n")
        for join in self.Joins:
            lCols = ", ".join(utils.getItems(self.TableColumns[join.LeftTableId], join.LeftColumnIds))
            rCols = ", ".join(utils.getItems(self.TableColumns[join.RightTableId], join.RightColumnIds))
            sb.write(f"JOIN t{join.LeftTableId}({lCols}) # t{join.RightTableId}({rCols}) -- {join.info}\n")
        for tableId, columnNames in zip(range(1024), self.TableColumns):
            sb.write(f"CUBE t{tableId}({', '.join(columnNames)})\n")
        for tableId, filters in sorted(self.TableFilters.items(), key=lambda it:it[0]):
            for filter in filters:
                sb.write(f"FILTER t{tableId}({', '.join(filter.columns)}) -- {filter.info}\n")
        sb.write(f"KEY {', '.join(('t'+str(c.TableId)+'.'+c.ColumnName for c in self.GlobalColumns))}")
        return sb.getvalue()

    
class CubeConf:
    pass

class CubeKey:
    def __init__(self, size:int) -> None:
        self.slots:List[Any] = [None for _ in range(size)]
    def set(self, index:int, value:Any) -> 'CubeKey':
        self.slots[index] = value
        return self
    def get(self, index:int) -> Any:
        return self.slots[index]
    def __getitem__(self, index:int) -> Any:
        return self.get(index)
    def __setitem__(self, index:int, value:Any) -> None:
        self.set(index, value)
    def __hash__(self) -> int:
        h = 0
        for e in self.slots:
            h += hash(e)
        return h
    def __len__(self) -> int:
        return len(self.slots)
    def __eq__(self, __value: 'CubeKey') -> bool:
        if len(self) != len(__value.slots):
            return False
        for i in range(len(self.slots)):
            e = self.slots[i]
            e1 = __value.slots[i]
            if e != e1:
                return False
        return True
    def copy(self) -> 'CubeKey':
        c = CubeKey(len(self.slots))
        c.slots[:] = self.slots[:]
        return c
    def populate(self, otherKey:Union['CubeKey', Tuple[Any, ...]], end:int)-> 'CubeKey':
        for i in range(end):
            assert self[i] == None, f"{self}, {otherKey}, {end}"
            self[i] = otherKey[i]
        return self
    def __str__(self) -> str:
        return "|".join((str(v) for v in self.slots))
    def __repr__(self) -> str:
        return str(self)
    @staticmethod
    def of(*values:Any) -> 'CubeKey':
        k = CubeKey(len(values))
        k.populate(values, len(values))
        return k
    
class CubeValue:
    def __init__(self, tableNumber:int) -> None:
        self.RowsetArray:List[List[int]] = [[] for _ in range(tableNumber)]
    def addRowId(self, tableId:TableId, rowId:Any) -> None:
        self.RowsetArray[tableId].append(int(rowId))
    def rowSize(self) -> int:
        s = 1
        for rows in self.RowsetArray:
            s *= len(rows)
        return s
    def copy(self) -> 'CubeValue':
        cp = CubeValue(len(self.RowsetArray))
        for tableId, rowIds in zip(range(1024), self.RowsetArray):
            cp.RowsetArray[tableId].extend(rowIds)
        return cp
    def __str__(self) -> str:
        return str(self.RowsetArray)
    def __repr__(self) -> str:
        return str(self)

class utils:
    @staticmethod
    def tableFlitrate(filters:List[TableFilter], row:List[Any]) -> bool:
        for filter in filters:
            values = [row[localId] for localId in filter.ColumnIds]
            if not filter.Predicate(values):
                return False
        return True
    @staticmethod
    def joinFlitrate(joins:List[Join], leftKey:CubeKey, rightKey:CubeKey) -> bool:
        for join in joins:
            values = utils.getItems(leftKey, join.LeftGlobalColumnIds) + utils.getItems(rightKey, join.rightGlobalColumnIds)
            if not join.Predicate(values):
                return False
        return True
    @staticmethod
    def indexes(li:List[T], es:List[T]) -> List[int]:
        r = []
        for e in es:
            r.append(li.index(e))
        return r
    @staticmethod
    def getItems(li:Union[List[T], CubeKey], indexes:List[int]) -> List[T]:
        r = []
        for i in indexes:
            r.append(li[i])
        return r

def doFullHypercube(info:CubeInfo, DB:Database) -> Dict[CubeKey, CubeValue]:
    hypercube:Dict[CubeKey, CubeValue] = dict() # 单表时，使用。最终返回
    joinHypercube:Dict[CubeKey, CubeValue] = dict() # join 时临时使用
    for tableId, tableName in zip(range(1024), info.Tables):
        columns = info.TableColumns[tableId] # 表名
        print(f"SCAN TABLE t{tableId} = {tableName} ({', '.join(columns)})")
        globalColumnIds = info.getGlobalColumnIds(tableId=tableId, columnNames=columns) # 局部列ID:全局列ID
        filters = info.TableFilters.get(tableId, []) 
        joins = info.getJoins(tableId) # JOIN 的右表就是当前遍历的表
        rows = DB.select(tableName=tableName, columnNames=[RowC] + columns)
        for row in rows:
            if utils.tableFlitrate(filters, row[1:]): # 单表过滤
                key = info.newCubeKey()
                for localColumnId, value in zip(range(1024), row[1:]):
                    key[globalColumnIds[localColumnId]] = value # create key
                if tableId == 0: # first table
                    if key not in joinHypercube:
                        joinHypercube[key] = info.newRowSetList()
                    joinHypercube[key].addRowId(tableId, row[0])
                else: # join
                    for eachKey, cube in hypercube.items():
                        if utils.joinFlitrate(joins, eachKey, key):
                            combineKey = key.copy().populate(eachKey, globalColumnIds[0]) # key 合并
                            if combineKey not in joinHypercube:
                                joinHypercube[combineKey] = cube.copy()
                            joinHypercube[combineKey].addRowId(tableId, row[0])
        hypercube = joinHypercube
        joinHypercube = dict() # clear
    return hypercube

