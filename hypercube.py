from sqlite import Database
from typing import Dict, List, Callable, Tuple

class TableFilter:
    # 表内过滤，例如 t0.a=10 或者 t0.a=t0.b 或者 t0.a=(t0.b+t0.c)
    def __init__(self, columnIds:List[int], predicate:Callable[[List[any]], bool]) -> None:
        self.ColumnIds = columnIds # 列ID
        self.Predicate = predicate

class TiCi: # tableId-columnId
    def __init__(self, tableId:int, columnId:int) -> None:
        self.TableId = tableId # 表ID
        self.ColumnId = columnId # 列ID
    def __hash__(self) -> int:
        return self.TableId * 1024 + self.ColumnId
    def __eq__(self, __value: 'TiCi') -> bool:
        return self.TableId == __value.TableId and self.ColumnId == __value.ColumnId
    def __str__(self) -> str:
        return f"{self.TableId}-{self.ColumnId}"
    def __repr__(self) -> str:
        return str(self)

class CubeInfo:
    def __init__(self) -> None:
        self.Tables:List[str] = [] # 涉及的表，并按照顺序编号 t0/t1/t2
        self.Columns:List[List[str]] = [] # 每张表涉及的列，外层为表编号，内层为多个列名，并按顺序编号 c0/c1/c2
        self.TableFilter:Dict[int, List[TableFilter]] # 表内过滤。例如 t0.a=10 或者 t0.a=t0.b 或者 t0.a=(t0.b+t0.c)
        self.InnerJoins:Dict[int, Dict[int, TiCi]] # 等值连接过滤，例如 t1.a=t0.a、t2.a=t1.b。满足 InnerJoins[t][c].t = t' 满足 t > t'

class CubeConf:
    pass

class CubeKey:
    def __init__(self) -> None:
        self.TableColumnValue:List[Tuple[TiCi, any]] = []
    def set(self, tableColumn:TiCi, value:any) -> 'CubeKey':
        for item in self.TableColumnValue:
            if item[0] == tableColumn:
                raise Exception("double set")
        self.TableColumnValue.append((tableColumn, value))
        self.TableColumnValue.sort(key=lambda item:hash(item[0]))
        return self
    def __hash__(self) -> int:
        h = 0
        for k, v in self.TableColumnValue:
            h += hash(k) + hash(v)
        return h
    def __eq__(self, __value: 'CubeKey') -> bool:
        if len(self.TableColumnValue) != len(__value.TableColumnValue):
            return False
        for i in range(len(self.TableColumnValue)):
            k, v = self.TableColumnValue[i]
            k1, v1 = __value.TableColumnValue[i]
            if k != k1 or v != v1:
                return False
        return True
    def copy(self) -> 'CubeKey':
        c = CubeKey()
        c.TableColumnValue = self.TableColumnValue.copy()
        return c
    def __str__(self) -> str:
        return "|".join((str(v) for v in self.TableColumnValue))
    def __repr__(self) -> str:
        return "|".join((str(v[1]) for v in self.TableColumnValue))
    

def hypercube(info:CubeInfo, conf:CubeConf) -> Dict[CubeKey, List[int]]:
    pass

