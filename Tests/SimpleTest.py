import unittest
import Solution
from Utility.ReturnValue import ReturnValue
from Tests.abstractTest import AbstractTest
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk

'''
    Simple test, create one of your own
    make sure the tests' names start with test_
'''


class Test(AbstractTest):
    def test_Disk(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(2, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addDisk(Disk(3, "DELL", 10, 10, 10)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addDisk(Disk(1, "DELL", 10, 10, 10)),
                         "ID 1 already exists")

    def test_RAM(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(1, "find minimum value", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(2, "find minimum value", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addRAM(RAM(3, "find minimum value", 10)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addRAM(RAM(3, "find minimum value", 10)),
                         "ID 1 already exists")

    def test_Query(self) -> None:
        self.assertEqual(ReturnValue.OK, Solution.addQuery(Query(1, "DELL", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addQuery(Query(2, "DELL", 10)), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addQuery(Query(3, "DELL", 10)), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addQuery(Query(3, "DELL", 10)),
                         "ID 1 already exists")


# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
