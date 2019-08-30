import os
import unittest

from .. import gggrun
from . import _test_data_dir


class TestInputs(unittest.TestCase):
    slice_input_file = os.path.join(_test_data_dir, 'slice-i2s.in')
    slice_output_file = os.path.join(_test_data_dir, 'slice-i2s.in.mod')
    # use to check creating a new input file and using a dict to modify a file
    slice_check_file1 = os.path.join(_test_data_dir, 'slice-i2s.in.chk1')
    # use to check overwriting an input file
    slice_check_file2 = os.path.join(_test_data_dir, 'slice-i2s.in.chk2')
    # use to check writing a multi-line parameter
    slice_check_file3 = os.path.join(_test_data_dir, 'slice-i2s.in.chk3')

    @classmethod
    def setUpClass(cls) -> None:
        with open(cls.slice_check_file1, 'r') as fobj:
            cls.slice_chk_str1 = fobj.read()
        with open(cls.slice_check_file2, 'r') as fobj:
            cls.slice_chk_str2 = fobj.read()
        with open(cls.slice_check_file3, 'r') as fobj:
            cls.slice_chk_str3 = fobj.read()

    def test_mod_i2s_input(self):
        if os.path.isfile(self.slice_output_file):
            # Delete existing output file, want to test creating new file
            os.remove(self.slice_output_file)

        # First create a new i2s input file with several parameters modified
        gggrun.modify_i2s_input_params(self.slice_input_file, 1, './igms/', 5, '1', 6, './phase_curves/',
                                       new_file=self.slice_output_file)
        with open(self.slice_output_file) as fobj:
            chk_str = fobj.read()
            self.assertEqual(chk_str, self.slice_chk_str1, msg='Creating new file with positional args failed')

        # Next overwrite that file, modifying two more parameters
        gggrun.modify_i2s_input_params(self.slice_output_file, 8, './flimit_new.i2s', 25, '0.005  0.005')
        with open(self.slice_output_file) as fobj:
            chk_str = fobj.read()
            self.assertEqual(chk_str, self.slice_chk_str2, msg='Overwriting existing file with positional args failed')

        # Next, test passing arguments as a dictionary - we'll revert to the first modified file
        gggrun.modify_i2s_input_params(self.slice_output_file, {8: './flimit.i2s', 25: '0.001  0.001'})
        with open(self.slice_output_file) as fobj:
            chk_str = fobj.read()
            self.assertEqual(chk_str, self.slice_chk_str1, msg='Overwriting existing file with dictionary arg failed')

        # Finally, test modifying a multiline option
        param_17 = '-5.00 -5.00   Min igram Thresh (Master, Slave)\n+5.00 +5.00   Max igram Thresh (Master, Slave)'
        gggrun.modify_i2s_input_params(self.slice_output_file, 17, param_17)
        with open(self.slice_output_file) as fobj:
            chk_str = fobj.read()
            self.assertEqual(chk_str, self.slice_chk_str3, msg='Writing a multiline parameter (parameter 17) failed')


if __name__ == '__main__':
    unittest.main()
