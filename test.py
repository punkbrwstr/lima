import unittest
import pandas as pd
import numpy as np
import lima

class LimaTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._lima = lima.Lima(password='gdaa')

    @classmethod
    def tearDownClass(cls):
        pass

class FrameTest(LimaTest):
    def setUp(self):
        f = pd.DataFrame(np.arange(12).reshape(3,4),columns=['a','b','c','d'],
                            index=pd.date_range('1/1/2019',periods=3,freq='B'))
        FrameTest._lima.write_frame('test', f)

    def tearDown(self):
        FrameTest._lima.delete_frame('test')
        
    def test_read(self):
        f = FrameTest._lima.read_frame('test')
        self.assertEqual(f.shape[1], 4)
        self.assertEqual(f.shape[0], 3)
        self.assertEqual(f.sum().sum(), 66)

    def test_append_row(self):
        f = pd.DataFrame(np.arange(4).reshape(1,4),columns=['a','b','c','d'],
                            index=pd.date_range('1/4/2019',periods=1,freq='B'))
        FrameTest._lima.write_frame('test', f)
        f = FrameTest._lima.read_frame('test')
        self.assertEqual(f.shape[1], 4)
        self.assertEqual(f.shape[0], 4)
        self.assertEqual(f.sum().sum(), 72)

    def test_append_row(self):
        f = pd.DataFrame(np.arange(3).reshape(3,1),columns=['e'],
                            index=pd.date_range('1/1/2019',periods=3,freq='B'))
        FrameTest._lima.write_frame('test', f)
        f = FrameTest._lima.read_frame('test')
        self.assertEqual(f.shape[1], 5)
        self.assertEqual(f.shape[0], 3)
        self.assertEqual(f.sum().sum(), 69)

class SeriesTest(LimaTest):
    def setUp(self):
        s = pd.Series(np.arange(3), index=pd.date_range('1/1/2019',periods=3,freq='B'))
        SeriesTest._lima.write_series('test', s)

    def tearDown(self):
        SeriesTest._lima.delete_series('test')
        
    def test_read(self):
        s = SeriesTest._lima.read_series('test')
        self.assertEqual(len(s), 3)
        self.assertEqual(s.sum(), 3)

    def test_read_before(self):
        s = SeriesTest._lima.read_series('test', '2018-12-31')
        self.assertEqual(len(s), 4)
        self.assertEqual(s.iloc[0], 0)

    def test_read_after(self):
        s = SeriesTest._lima.read_series('test', end='2019-01-05')
        self.assertEqual(len(s), 4)
        self.assertEqual(s.iloc[-1], 0)

    def test_append(self):
        a = pd.Series(np.arange(3), index=pd.date_range('1/4/2019',periods=3,freq='B'))
        SeriesTest._lima.write_series('test', a)
        s = SeriesTest._lima.read_series('test')
        self.assertEqual(len(s), 6)
        self.assertEqual(s.sum(), 6)

    def test_append_with_pad(self):
        a = pd.Series(np.arange(3), index=pd.date_range('1/5/2019',periods=3,freq='B'))
        SeriesTest._lima.write_series('test', a)
        s = SeriesTest._lima.read_series('test')
        self.assertEqual(len(s), 7)
        self.assertEqual(s.sum(), 6)

    def test_replace(self):
        a = pd.Series(10, index=pd.date_range('1/2/2019',periods=1,freq='B'))
        SeriesTest._lima.write_series('test', a)
        s = SeriesTest._lima.read_series('test')
        self.assertEqual(len(s), 3)
        self.assertEqual(s.sum(), 12)

    def test_prepend(self):
        a = pd.Series(np.arange(3), index=pd.date_range('12/31/2018',periods=3,freq='B'))
        SeriesTest._lima.write_series('test', a)
        s = SeriesTest._lima.read_series('test')
        self.assertEqual(len(s), 3)
        self.assertEqual(s.sum(), 3)

if __name__ == '__main__':
    unittest.main()
