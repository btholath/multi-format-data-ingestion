# tests/test_orc_utils.py

import os
import tempfile
from utils.orc_utils import write_orc, read_orc

def test_write_and_read_orc():
    orc_schema = "struct<policy_id:string,holder_name:string,age:int,sum_assured:int,premium:int,term:int,plan:string>"
    test_tuples = [("P1", "Orc User", 31, 120000, 950, 8, "Whole Life")]
    with tempfile.TemporaryDirectory() as tmpdir:
        orc_file = os.path.join(tmpdir, "test.orc")
        write_orc(test_tuples, orc_file, orc_schema)
        records, schema = read_orc(orc_file)
        assert len(records) == 1
        assert records[0][1] == "Orc User"
        assert str(schema) == orc_schema
        print("test_write_and_read_orc PASSED")

if __name__ == "__main__":
    test_write_and_read_orc()
