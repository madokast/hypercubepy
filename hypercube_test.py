from hypercube import CubeKey, TiCi
from operator import methodcaller

class test_CubeKey:
    def test01(self):
        m = dict()
        m[CubeKey().set(TiCi(0, 0), "a")] = 10
        print(m)
    def test02(self):
        m = dict()
        m[CubeKey().set(TiCi(0, 0), "a")] = 10
        m[CubeKey().set(TiCi(0, 0), "b")] = 20
        print(m)
    def test03(self):
        m = dict()
        m[CubeKey().set(TiCi(1, 0), "a").set(TiCi(0, 0), "b")] = 10
        m[CubeKey().set(TiCi(0, 0), "a").set(TiCi(1, 0), "b")] = 20
        print(m)
        print(CubeKey().set(TiCi(1, 0), "a").set(TiCi(0, 0), "b"))
        print(CubeKey().set(TiCi(0, 0), "a").set(TiCi(1, 0), "b"))

def test(testcase):
    cls = str(type(testcase))[len("<class '__main__)"):]
    for f in dir(testcase):
        if f.startswith("test"):
            print(cls[:-2], f)
            methodcaller(f)(testcase)
    

if __name__ == '__main__':
    test(test_CubeKey())