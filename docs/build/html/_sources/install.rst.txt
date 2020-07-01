Installation
============

GGGUtils can be obtained from Github at https://github.com/joshua-laughner/GGGUtils.

    * Option 1 (with git): clone with ``git clone https://github.com/joshua-laughner/GGGUtils.git``
    * Option 2 (without git): go to the `repo <https://github.com/joshua-laughner/GGGUtils>`_, click on the "Clone"
      button, and choose "Download ZIP". Once it finishes downloading, unzip and place wherever you wish.

This follows the standard Python installation, in the top directory, there is a :file:`setup.py` file. The recommended
way to install is to execute the command::

    python setup.py --user develop

or, if installing into an environment:

    python setup.py develop

in this directory. This is preferred over ``python setup.py install`` because it creates an "editable" install where
changes to this copy are immediately active, without needing to run ``python setup.py install`` again.


Dependencies
------------

For ``i2srun`` (which is the only officially supported part of this package at present) only two non-standard packages
are required:

    * `ConfigObj <https://configobj.readthedocs.io/en/latest/configobj.html>`_
    * `TextUI <https://pypi.org/project/textui/>`_

These are included as dependencies in the :file:`setup.py` and so should be automatically installed.

What is installed
-----------------

In addition to the ``gggutils`` Python package, this installs a command line script, ``gggutils`` as an entry point
to the various command line programs. During installation, watch for a line similar to::

    Installing gggutils script to /home/josh/.local/bin

as this is where that script goes. For it to be accessible globally, ensure that that directory is on your shell's PATH.
If you install it in a virtual environment or Conda environment, activating that environment will modify your PATH
automatically.


Is Python 2 supported?
----------------------

Nope. `Python 2 has reached end of life <https://www.python.org/doc/sunset-python-2/>`_. It's time to upgrade. (If you
really can't upgrade, I recommend creating a Python 3 conda environment.)