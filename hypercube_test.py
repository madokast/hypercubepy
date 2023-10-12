from hypercube import CubeKey, CubeInfo, TableFilter, Join, doFullHypercube, CubeConf, CubeValue
from operator import methodcaller
from sqlite import Database, RowC
from typing import Dict

class test_CubeKey:
    def test01(self):
        m = dict()
        m[CubeKey(2).set(0, "a").set(1, "b")] = [1,2,3]
        m[CubeKey(2).set(0, "a").set(1, "c")] = [2,3,4]
        print(m)
    def test02(self):
        m = dict()
        ck = CubeKey(2)
        ck[0] = "a"
        ck[1] = 1
        m[ck] = [1,2,3]
        ck = ck.copy()
        ck[1] = 2
        m[ck] = [4,5,6]
        print(m)

class test_CubeInfo:
    def test01(self): # stu(name, age)
        info = CubeInfo()
        info.Tables = ["stu"]
        info.TableColumns = [["name", "age"]]
        info.preprocess()
        print(info)
    def test02(self): # stu(name, age) age > 20 and age < 50
        info = CubeInfo()
        info.setTables("stu")
        info.setColumns(0, "name", "age")
        info.addFilter(TableFilter(tableId=0, columns=['age'], predicate=lambda vs:int(vs[0])>20, info="age>20", cubeInfo=info))
        info.addFilter(TableFilter(tableId=0, columns=['age'], predicate=lambda vs:int(vs[0])<50, info="age<50", cubeInfo=info))
        print(info)
    def test03(self): # stu(name, age) t0, stu(name, age) t1, t0.age > t1.age
        info = CubeInfo()
        info.setTables("stu", "stu")
        info.setColumns(0, "name", "age")
        info.setColumns(1, "age", "name")
        info.addJoin(join=Join(leftTableId=0, leftColumnNames=['age'], rightTableId=1, rightColumnNames=['age'],
                               predicate=lambda v:int(v[0])>int(v[1]), info='age>age', cubeInfo=info, isInnerJoin=False))
        print(info)

class test_doFullHypercubeSingleTable:
    def test01(self): # stu(name)
        DB = Database()
        utils.createTableStu(DB)
        info = CubeInfo()
        info.setTables("stu")
        info.setColumns(0, "name")
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        assert hc[CubeKey.of('a')].RowsetArray == [[0, 1, 5]]
        assert hc[CubeKey.of('b')].RowsetArray == [[2, 3]]
        assert hc[CubeKey.of('c')].RowsetArray == [[4]]
    def test02(self): # stu(age)
        DB = Database()
        utils.createTableStu(DB)
        info = CubeInfo()
        info.setTables("stu")
        info.setColumns(0, "age")
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        assert hc[CubeKey.of('15')].RowsetArray == [[0, 2, 5]]
        assert hc[CubeKey.of('20')].RowsetArray == [[1, 3, 4]]
    def test03(self): # stu(age, name)
        DB = Database()
        utils.createTableStu(DB)
        info = CubeInfo()
        info.setTables("stu")
        info.setColumns(0, "age", "name")
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        assert hc[CubeKey.of('15', 'a')].RowsetArray == [[0, 5]]
        assert hc[CubeKey.of('20', 'a')].RowsetArray == [[1]]
        assert hc[CubeKey.of('15', 'b')].RowsetArray == [[2]]
        assert hc[CubeKey.of('20', 'b')].RowsetArray == [[3]]
        assert hc[CubeKey.of('20', 'c')].RowsetArray == [[4]]
    def test04(self): # stu(name, age)
        DB = Database()
        utils.createTableStu(DB)
        info = CubeInfo()
        info.setTables("stu")
        info.setColumns(0, "name", "age")
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        assert hc[CubeKey.of('a','15',)].RowsetArray == [[0, 5]]
        assert hc[CubeKey.of('a','20',)].RowsetArray == [[1]]
        assert hc[CubeKey.of('b','15',)].RowsetArray == [[2]]
        assert hc[CubeKey.of('b','20',)].RowsetArray == [[3]]
        assert hc[CubeKey.of('c','20',)].RowsetArray == [[4]]
    def test05(self): # stu(rid)
        DB = Database()
        utils.createTableStu(DB)
        info = CubeInfo()
        info.setTables("stu")
        info.setColumns(0, RowC)
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        for i in range(DB.tableLength('stu')):
            assert hc[CubeKey.of(i)].RowsetArray == [[i]]
    def test06(self): # stu(rid) rid>2
        DB = Database()
        utils.createTableStu(DB)
        info = CubeInfo()
        info.setTables("stu")
        info.setColumns(0, RowC)
        info.addFilter(TableFilter(0, [RowC], lambda vs:vs[0]>2, "row>2", info))
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        assert len(hc) == 3
        for i in range(3, DB.tableLength('stu')):
            assert hc[CubeKey.of(i)].RowsetArray == [[i]]
    def test07(self): # stu(age) age>15
        DB = Database()
        utils.createTableStu(DB)
        info = CubeInfo()
        info.setTables("stu")
        info.setColumns(0, 'age')
        info.addFilter(TableFilter(0, ['age'], lambda vs:int(vs[0])>15, "age>15", info))
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        assert len(hc) == 1
        assert hc[CubeKey.of('20')].RowsetArray == [[1, 3, 4]]
    def test08(self): # trs(s, a, b) s=a+b
        DB = Database()
        utils.createTableTrs(DB)
        info = CubeInfo()
        info.setTables("trs")
        info.setColumns(0, 's', 'a', 'b')
        info.addFilter(TableFilter(0, ['s', 'a', 'b'], lambda vs:int(vs[0])==int(vs[1])+int(vs[2]), "s=a+b", info))
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        assert len(hc) == 4
        assert hc[CubeKey.of('10', '5', '5')].RowsetArray == [[0]]
        assert hc[CubeKey.of(*"20|15|5".split("|"))].RowsetArray == [[1]]
        assert hc[CubeKey.of(*"40|12|28".split("|"))].RowsetArray == [[3]]
        assert hc[CubeKey.of(*"50|20|30".split("|"))].RowsetArray == [[4]]

class test_doFullHypercubeCrossTable:
    def test01(self): # stu t0, stu t1
        DB = Database()
        utils.createTableStu(DB)
        DB.conn.execute("CREATE VIEW j AS SELECT * FROM stu t0, stu t1;")
        info = CubeInfo()
        info.setTables("stu", 'stu')
        info.setColumns(0, "age")
        info.setColumns(1, "age")
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        print("row-size", utils.rowSize(hc), DB.tableLength('j'))
        assert len(hc) == 4
        assert utils.rowSize(hc) == DB.tableLength('j')
        assert hc[CubeKey.of(*'15|15'.split('|'))].RowsetArray == [[0, 2, 5], [0, 2, 5]]
        assert hc[CubeKey.of(*'20|15'.split('|'))].RowsetArray == [[1, 3, 4], [0, 2, 5]]
        assert hc[CubeKey.of(*'15|20'.split('|'))].RowsetArray == [[0, 2, 5], [1, 3, 4]]
        assert hc[CubeKey.of(*'20|20'.split('|'))].RowsetArray == [[1, 3, 4], [1, 3, 4]]
    def test02(self): # stu t0, stu t1, t1.age>15
        DB = Database()
        utils.createTableStu(DB)
        DB.conn.execute("CREATE VIEW j AS SELECT * FROM stu t0, stu t1 WHERE t1.age>15;")
        info = CubeInfo()
        info.setTables("stu", 'stu')
        info.setColumns(0, "age")
        info.setColumns(1, "age")
        info.addFilter(TableFilter(1, ['age'], lambda v:int(v[0])>15, 'age>15', info))
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        print("row-size", utils.rowSize(hc), DB.tableLength('j'))
        assert len(hc) == 2
        assert utils.rowSize(hc) == DB.tableLength('j')
        # assert hc[CubeKey.of(*'15|15'.split('|'))].RowsetArray == [[0, 2, 5], [0, 2, 5]]
        # assert hc[CubeKey.of(*'20|15'.split('|'))].RowsetArray == [[1, 3, 4], [0, 2, 5]]
        assert hc[CubeKey.of(*'15|20'.split('|'))].RowsetArray == [[0, 2, 5], [1, 3, 4]]
        assert hc[CubeKey.of(*'20|20'.split('|'))].RowsetArray == [[1, 3, 4], [1, 3, 4]]
    def test03(self): # stu t0, stu t1, t1.age>15 t0.age<20
        DB = Database()
        utils.createTableStu(DB)
        DB.conn.execute("CREATE VIEW j AS SELECT * FROM stu t0, stu t1 WHERE t1.age>15 and t0.age<20;")
        info = CubeInfo()
        info.setTables("stu", 'stu')
        info.setColumns(0, "age")
        info.setColumns(1, "age")
        info.addFilter(TableFilter(1, ['age'], lambda v:int(v[0])>15, 'age>15', info))
        info.addFilter(TableFilter(0, ['age'], lambda v:int(v[0])<20, 'age<20', info))
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        print("row-size", utils.rowSize(hc), DB.tableLength('j'))
        assert len(hc) == 1
        assert utils.rowSize(hc) == DB.tableLength('j')
        # assert hc[CubeKey.of(*'15|15'.split('|'))].RowsetArray == [[0, 2, 5], [0, 2, 5]]
        # assert hc[CubeKey.of(*'20|15'.split('|'))].RowsetArray == [[1, 3, 4], [0, 2, 5]]
        assert hc[CubeKey.of(*'15|20'.split('|'))].RowsetArray == [[0, 2, 5], [1, 3, 4]]
        # assert hc[CubeKey.of(*'20|20'.split('|'))].RowsetArray == [[1, 3, 4], [1, 3, 4]]
    def test04(self): # stu t0, stu t1, t1.age=t0.age
        DB = Database()
        utils.createTableStu(DB)
        DB.conn.execute("CREATE VIEW j AS SELECT * FROM stu t0, stu t1 WHERE t0.age=t1.age;")
        DB.show('j')
        info = CubeInfo()
        info.setTables("stu", 'stu')
        info.setColumns(0, "age")
        info.setColumns(1, "age")
        info.addJoin(Join(0, ['age'], 1, ['age'], predicate=lambda v:int(v[0])==int(v[1]), info="age=age", cubeInfo=info, isInnerJoin=True))
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        print("row-size", utils.rowSize(hc), DB.tableLength('j'))
        assert len(hc) == 2
        assert utils.rowSize(hc) == DB.tableLength('j')
        assert hc[CubeKey.of(*'15|15'.split('|'))].RowsetArray == [[0, 2, 5], [0, 2, 5]]
        # assert hc[CubeKey.of(*'20|15'.split('|'))].RowsetArray == [[1, 3, 4], [0, 2, 5]]
        # assert hc[CubeKey.of(*'15|20'.split('|'))].RowsetArray == [[0, 2, 5], [1, 3, 4]]
        assert hc[CubeKey.of(*'20|20'.split('|'))].RowsetArray == [[1, 3, 4], [1, 3, 4]]
    def test05(self): # t0.a = t2.b ^ t2.c = t4.d
        DB = Database()
        DB.createTable("a", [('a', [1, 1, 2])])
        DB.createTable("bc", [('b', [1, 1, 3]), ('c', [2, 2, 2])])
        DB.createTable("d", [('d', [2, 2, 2])])
        DB.conn.execute("CREATE VIEW j AS SELECT * FROM a t0, bc t2, d t4 WHERE t0.a = t2.b AND t2.c = t4.d;")
        info = CubeInfo()
        info.setTables("a", "bc", "d")
        info.setColumns(0, "a")
        info.setColumns(1, "b", "c")
        info.setColumns(2, "d")
        info.addJoin(Join(0, ['a'], 1, ['b'], predicate=lambda v:int(v[0])==int(v[1]), info="a=b", cubeInfo=info, isInnerJoin=True))
        info.addJoin(Join(1, ['c'], 2, ['d'], predicate=lambda v:int(v[0])==int(v[1]), info="c=d", cubeInfo=info, isInnerJoin=True))
        print(info)
        hc = doFullHypercube(info, DB)
        print(hc)
        print("row-size", utils.rowSize(hc), DB.tableLength('j'))
        assert len(hc) == 1
        assert utils.rowSize(hc) == DB.tableLength('j')
        assert hc[CubeKey.of(*'1|1|2|2'.split('|'))].RowsetArray == [[0, 1], [0, 1], [0, 1, 2]]

class utils:
    @staticmethod
    def createTableStu(DB:Database):
        DB.createTable("stu", [
            ("name", ['a', 'a', 'b', 'b', 'c', 'a']),
            ("age", [15, 20, 15, 20, 20, 15])
        ])
        DB.show('stu')
    @staticmethod
    def createTableTrs(DB:Database):
        DB.createTable("trs", [
            ("date", ['1-1', '1-2', '1-3', '2-10', '2-15', '3-3']),
            ("s", [10, 20, 30, 40, 50, 60]),
            ("a", [5,  15, 20, 12, 20, 15]),
            ("b", [5,  5,   8, 28, 30, 40]),
        ])
        DB.show("trs")
    @staticmethod
    def rowSize(hc:Dict[CubeKey, CubeValue]) -> int:
        s = 0
        for v in hc.values():
            s += v.rowSize()
        return s

def test(testcase):
    cls = str(type(testcase))[len("<class '__main__)"):]
    for f in dir(testcase):
        if f.startswith("test"):
            print('----------------------------------',cls[:-2], f)
            methodcaller(f)(testcase)
            print()

if __name__ == '__main__':
    test(test_CubeKey())
    test(test_CubeInfo())
    test(test_doFullHypercubeSingleTable())
    test(test_doFullHypercubeCrossTable())