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

To test if you have Python 3 installed, run the following in the terminal::

    python -V

If the output is something like ``Python 3.8.3`` (as long as the first number is a 3), you're all set. If instead it is
``Python 2.7.12`` or similar (the first number is a 2), your default python is Python 2. You can try::

    python3 -V

(note the "3" in the command). If this returns ``Python 3.x.x`` then you can proceed by substituting ``python3`` for
``python`` in the install commands. If it returns ``Command not found``, then you will need to upgrade or create a
Python 3 environment.


Creating a Python 3 environment
*******************************

This assumes you have Anaconda Python installed, specifically the ``conda`` package manager. You can verify this with::

    conda -V

If this returns ``Command not found``, you will need to `install Anaconda <https://www.anaconda.com/products/individual>`_.
Currently, both Python 2 and Python 3 Anaconda distributions are available; you may install a Python 2 Anaconda to keep
Python 2 as your default; it will still be able to create Python 3 environments.

To create a new Python 3 environment, run::

    conda create -n gggutils python=3

The value "gggutils" after the ``-n`` may be whatever name you wish to use for this environment; I used "gggutils" here
since that is the package we created this environment for.

You will then need to activate this environment before installing GGGUtils. To do so, the command is::

    conda activate gggutils

If you named the environment something other than "gggutils", use that as the last word instead. Then install GGGUtils
with the "installing in an environment" command in the first section.

To use GGGUtils once it is installed in this environment, you have two options:

    1. Remember to activate this environment before calling the ``gggutils`` command line program.
    2. Move the ``gggutils`` script created by the installation into a directory already on your PATH. If you already
       have a directory for command line scripts accessible from anywhere, that is a good place for it. Do not add the
       directory where it is installed to your PATH; that directory contains all command line programs for the "gggutils"
       environment, and so adding it to your PATH may cause you to use other programs from that environment (including
       python itself) even when the environment isn't activated.

You can deactivate this environment to return to using your default Python with the command ``conda deactivate`` or by
simply logging into a new shell.