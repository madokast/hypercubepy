import sqlite3
from typing import Any, List, Tuple

class Database:
    def __init__(self) -> None:
        self.conn = sqlite3.connect(":memory:")
        self.ridColumnName = 'rid'

    def createTable(self, tableName:str, columns:List[Tuple[str, List[Any]]]) -> None:
        sql = f"CREATE TABLE {tableName} (\n"
        sql += f"{self.ridColumnName} INT PRIMARY KEY NOT NULL,\n"
        for column in columns:
            sql += f"{column[0]} TEXT NOT NULL,\n"
        sql = sql[:len(sql)-2] + ");"
        self.conn.execute(sql)

        for i in range(len(columns[0][1])):
            values = f"{i},"
            for column in columns:
                values += f"'{column[1][i]}',"
            values = values[:len(values)-1]
            sql = f"INSERT INTO {tableName} VALUES ({values});"
            self.conn.execute(sql)

    def selectOne(self, tableName:str, columnName:str, rid:int) -> str:
        cr = self.conn.execute(f"SELECT {columnName} FROM {tableName} WHERE {self.ridColumnName} = {rid}")
        for row in cr:
            return row[0]
        cr.close()
    
    def selectColumnRange(self, tableName:str, columnName:str, startRid:int, endRid:int) -> List[str]:
        cr = self.conn.execute(f"SELECT {columnName} FROM {tableName} WHERE {self.ridColumnName} >= {startRid} AND {self.ridColumnName} < {endRid}")
        r = [row[0] for row in cr]
        cr.close()
        return r
    
    def columnNames(self, tableName:str) -> List[str]:
        cr = self.conn.execute(f"PRAGMA table_info({tableName});")
        r = [row[1] for row in cr]
        cr.close()
        return r
    
    def show(self, tableName) -> None:
        columns = self.columnNames(tableName)
        print(*columns, sep='\t')
        cr = self.conn.execute(f"SELECT * FROM {tableName}")
        for row in cr:
            print(*row, sep='\t')
        

if __name__ == '__main__':
    DB = Database()
    DB.createTable("stu", [("name", ["aa", "dd", "er"]), ("age", [20, 30, 40])])
    print(DB.selectOne("stu", "name", 0))
    print(DB.selectOne("stu", "name", 1))
    print(DB.selectOne("stu", "name", 2))

    print(DB.selectOne("stu", "age", 0))
    print(DB.selectOne("stu", "age", 1))
    print(DB.selectOne("stu", "age", 2))

    print(DB.selectColumnRange("stu", "age", 0, 10))
    print(DB.selectColumnRange("stu", "name", 0, 2))

    print(DB.columnNames("stu"))

    DB.show("stu")
    