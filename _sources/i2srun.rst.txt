I2SRun - Utilities to run I2S in batch
======================================

``i2srun`` is a collection of utilities that make it easier to run I2S in batch. The standard approach is to use it to
set up a collection of run directories along with a shell file that can be passed to GNU parallel to run I2S in each
directory in parallel. Specifically the steps are:

    1. (optional) Create a header file (with all the common I2S options for your site) and a catalog file (with the list
       of scans for your site).
    2. Create separate I2S input files for each day, month, or year, and a config file that tells ``i2srun`` where to
       find your interferograms, which flimit file to use, etc.
    3. Modify the config file with the necessary settings for your site.
    4. Create the run directories, one per day, month, or year that you wish to run in parallel.
    5. Run multiple I2S instances with GNU Parallel.

Alternately, if you do not have GNU Parallel installed, ``i2srun`` has a mechanism to run I2S in parallel itself
(replacing step 5) but using GNU Parallel is preferred.

Note that for steps 1 and 2 you must work on one site at a time. Once you have the config and input files from step 2,
then you can combine the config files from multiple sites from step 3 on to allow parallelization over multiple sites.

Now we will go through each step in more detail.

Step 1 - Create header and catalog files
----------------------------------------

This can either be done manually or with the ``i2srun`` subcommand ``header-catalog``, or ``hc`` for short. The goal is
to produce two files: the header, which includes all the common I2S options shown
`on the TCCON wiki <https://tccon-wiki.caltech.edu/Software/GGG/Download/GGG_2020_Release_Notes/I2S_2020_Release_Notes#Common_I2S_input_parameters>`_,
and the catalog of scans to process. For sites that record slices, this will be a list of year, month, day, run, and
starting slice values. For Opus interferograms, this will be the list of interferogram files plus the ancillary data
needed.

Both of these files can be created manually, or with existing tools. Alternatively, if you have many preexisting I2S
input files that you wish to generate the header and catalog from, ``i2srun`` provides a utility to do so,
``header-catalog``. The command::

    gggutils i2s header-catalog xx_i2s_header.in xx_i2s_catalog.in *.i2s.in

would read all the I2S input files matching the pattern ``*.i2s.in`` and write the header to ``xx_i2s_header.in`` and
the catalog to ``xx_i2s_catalog.in``.

Note that this step is not necessary, depending on how you generate the config file, but you may find it useful, but it
is the most convenience way to have a single place to modify your general I2S options.

Step 2 - Create parallel I2S input files and the i2srun config file
-------------------------------------------------------------------

The next step is to create a configuration file that ``i2srun`` can use to figure out how to parallelize your I2S runs
and the individual I2S input files for running in parallel. This can be done in two ways: either using a header +
catalog file pair or an existing collection of I2S input files. An example of the first method is::

    gggutils i2s build-cfg-hc xx ./i2srun-config xx_i2s_header.in xx_i2s_catalog.in

This will create the configuration and parallel input files for site "xx" in the :file:`i2srun-config` directory, using
:file:`xx_i2s_header.in` to set the general I2S options for all the parallel input files and :file:`xx_i2s_catalog.in`
to figure out which interferograms exist to be processed.

.. note::
   The "xx" in the header and catalog file names need not correspond to the "xx" used for the site ID. Your header
   and catalog files may be named anything.

An example of the second method is::

    gggutils i2s build-cfg-many xx ./i2srun-config *.i2s.in

This will automatically extract a catalog of interferograms from all the input files passed (those matching ``*.i2s.in``)
and take the header from the first of those files. Exactly like the first method, a config file and individual I2S
input files will be placed in the directory :file:`i2srun-config`.

Both of these methods have the ``--split-by`` option, which controls how finely divided the interferograms should be
for parallel processing. The default is to split them up so that each day will be run separately, but they can also be
grouped by month or year by setting the value of ``--split-by`` to ``M`` or ``Y``, respectively.

Step 3 - Modify the config file as necessary for your site
----------------------------------------------------------

The third step is to modify the configuration file so that ``i2srun`` knows how to set up the separate, parallel I2S
runs. Details of the configuration file follow, but generally the minimum you need to do is:

    1. Set ``run_top_dir`` in the ``[Run]`` section to the location where you want your I2S runs to happen.
    2. For each site in the ``[Sites]`` section, set:
        * ``slices``: whether it uses slices or not
        * ``site_root_dir``: the path to the directory where your slice date folders (i.e. the :file:`YYMMDD.R` folders)
          or your interferograms are.
        * ``flimit_file``: path to the flimit file to use for this site.
        * Set ``no_date_dir`` to ``True`` or ``1``
        * Set ``subdir`` to ``.``
        * Set ``slices_in_subdir`` to ``False`` or ``0``
    3. The ``i2s_input_file`` values for each year/month/day should be fine as their defaults, unless you move the
       config or generated I2S input files.

.. note::
   The four options that take paths (``run_top_dir``, ``site_root_dir``, ``flimit_file``, and ``i2s_input_file``)
   interpret relative paths as *relative to the config file*. That is, if the ``i2s_input_file`` option is
   :file:`./demo.i2s`, then ``i2srun`` always looks for it in the same directory as the config file, *not* the
   directory you execute ``i2srun`` from.

Config file details
###################

This section will give the full details of the config file. Here is an example config file::

    [Run]
    # The directory where the data are linked to to run I2S/GGG
    run_top_dir = /oco2-data/tccon-nobak/scratch/beta-test-spectra/rc1

    [I2S]
    3 = 0 # do not save separated interferograms
    5 = 0 # do not save phase curves
    17 = -1.00 -1.00\n+1.00 +1.00 # update the extremes allows for the igrams values
    21 = 8388608 8388608 #update the max log-base-2 num igram points
    25 = 0.001 0.001 # update the PCT threshold

    [Sites]
    [[pa]]
    slices = True
    site_root_dir = /oco2-data/tccon/data/parkfalls_ifs1
    no_date_dir = True
    subdir = .
    slices_in_subdir = False
    flimit_file = /home/jlaugh/GGG/from-matt/flimit-files/pa_flimit.i2s

    [[[pa20140918]]]
    i2s_input_file = /home/jlaugh/GGG/GGG2019-beta/rc1/i2s-run-files/slice-i2s.pa20140918.in
    [[[pa20140925]]]
    i2s_input_file = /home/jlaugh/GGG/GGG2019-beta/rc1/i2s-run-files/slice-i2s.pa20140925.in
    [[[pa20140927]]]
    i2s_input_file = /home/jlaugh/GGG/GGG2019-beta/rc1/i2s-run-files/slice-i2s.pa20140927.in

    [[wg]]
    slices = False
    site_root_dir = /home/jlaugh/GGGData/WollongongTargetIgms/pseudo-target-dirs
    no_date_dir = False
    subdir = igms
    slices_in_subdir = False
    flimit_file = /home/jlaugh/GGG/from-matt/flimit-files/wg_flimit.i2s
    [[[wg20140923]]]
    i2s_input_file = /home/jlaugh/GGG/GGG2019-beta/rc1/i2s-run-files/opus-i2s.wg20140923.in
    [[[wg20160210]]]
    i2s_input_file = /home/jlaugh/GGG/GGG2019-beta/rc1/i2s-run-files/opus-i2s.wg20160210.in
    [[[wg20170424]]]
    i2s_input_file = /home/jlaugh/GGG/GGG2019-beta/rc1/i2s-run-files/opus-i2s.wg20170424.in


Notice that this follows a somewhat expanded `INI format <https://en.wikipedia.org/wiki/INI_file>`_. Sections are
denoted by names enclosed in ``[brackets]`` with subsections enclosed in ``[[multiple brackets]]``. In the above
example, ``[[pa]]`` is a subsection of ``[Sites]`` and ``[[[pa20140918]]]`` a subsection of ``[[pa]]``. Comments are
allowed, both on their own and inline, beginning with a ``#``. Details on the options for each section follow.

Run section
***********

This section controls the execution of I2S. Options that it must have are:

* ``run_top_dir`` - this is a path to where run directories for I2S can be created.

I2S section
***********

This section allows you to set options in the I2S input file. For each line, the key must be the parameter number
and the value the value it should take. In the above example, the line ``3 = 0`` sets Parameter #3 (whether to save
separated interferograms) to 0 for all I2S run files it creates in the run directories. If a parameter needs to be
on two lines (like Parameter #17) indicate the line break with a ``\n``.

.. note::
   This section should be left blank in normal usage. Generally it is more straightforward (and safer) to make the
   change in your header file for Step 2. This section is retained in ``i2srun`` only to simplify bulk testing of
   different I2S parameters on e.g. the OCO-2/3 target data.

Sites section
*************

This section controls which sites and days are to be run and how to run them. It is organized into subsections by site
ID, and sub-subsections by site ID + date in YYYYMMDD format. Each date to run must have the options listed below;
however, it is set up so that if an option is not present in the date sub-subsection, it is read from the site
subsection. As an example, consider::

    [[pa]]
    flimit_file = /home/jlaugh/GGG/from-matt/flimit-files/pa_flimit.i2s

    [[[pa20140918]]]
    i2s_input_file = /home/jlaugh/GGG/GGG2019-beta/rc1/i2s-run-files/slice-i2s.pa20140918.in
    flimit_file = /home/tccon/defaults/std_pa_flimit.i2s
    [[[pa20140925]]]
    i2s_input_file = /home/jlaugh/GGG/GGG2019-beta/rc1/i2s-run-files/slice-i2s.pa20140925.in

2014-09-18 would use the flimit file ``/home/tccon/defaults/std_pa_flimit.i2s`` because the ``flimit_file`` value in
that specific subsection takes precedence. However, since 2014-09-25 does not include the ``flimit_file`` option, I2SRun
goes up one level to the ``[[pa]]`` section and uses the ``flimit_file`` value there, in this case,
``/home/jlaugh/GGG/from-matt/flimit-files/pa_flimit.i2s``.

The required options are:

* ``slices`` - whether this site uses slices or Opus interferograms. Must be a boolean value: ``True`` or ``False``.
* ``site_root_dir`` - root directory where interferograms or slices for this site can be found. Because I2SRun was
  originally built for OCO-2 targets, it assumes a certain directory structure, which will be discussed more below.
* ``no_date_dir`` - whether the interferograms or slices are organized by date under the ``site_root_dir``. Must be a
  boolean value: ``True`` or ``False``.
* ``subdir`` - the subdirectory under the site root directory and/or date subdirectory where the interferograms or
  slices are actually found.
* ``slices_in_subdir`` - only matters if processing slices. Generally we assume that slices are organized under the
  subdirectory into :file:`YYMMDD.R/scan` directories (where YY is the year, MM the month, DD the day, and R the run
  number). This directory structure is automatically deduced. However, if your slices are *not* organized in this
  manner, then you can set this option to ``True`` to indicate that the slice files are to be found directly in the
  subdir. Examples below.
* ``flimit_file`` - path to the flimit file to use for I2S. Will be copied into the run directories.
* ``i2s_input_file`` - path to the I2S input file to use to run I2S. This option should be in the date-specific
  sub-subsection to make any sense.

In the following examples, we will use ``site_root_dir = /data/site`` and ``subdir = igrams``. First we will examine
the case where ``slices`` is ``False``, i.e. we're processing Opus interferograms.

* If ``no_date_dir`` is ``True``, then interferograms are expected to be in ``$ROOT/$SUBDIR`` e.g.
  :file:`/data/site/igrams`
* If ``no_date_dir`` is ``False``, then interferograms are expected to be in ``$ROOT/$DATEDIR/$SUBDIR``, e.g.
  :file:`/data/site/wg20180101/igrams`, where ``wg20180101`` came from the date sub-subsection name.

If ``slices`` is ``True``, then:

* The same rules for ``no_date_dir`` apply, that is, the front of the path is either ``$ROOT/$SUBDIR`` or
  ``$ROOT/$DATEDIR/$SUBDIR``. Whichever is the case, call that ``$DATADIR``.
* Then, if ``slices_in_subdir`` is ``False``, the slices are assumed to be in ``$DATADIR/YYMMDD.R/scan``.
* If ``slices_in_subdir`` is ``True``, then the slices are assumed to be in ``$DATADIR`` directly.

.. note::
   In the current version, the sub-subsection names are expected to consist of the two letter site ID followed by
   between 4 and 8 digits giving the year, year & month, or year-month-day. At this time, no other format is permitted.


Running multiple sites
######################

To run multiple sites in parallel, you must first do Step 1 and 2 separately for each site. Then take the separate
config files produced and copy the site subsections into a single config file. In the above example, note how the
``[Sites]`` section has two subsections: ``[[pa]]`` and ``[[wg]]``. To get this, you would do steps 1 and 2 for
Park Falls and Wollongong separately, then e.g. copy the ``[[wg]]`` subsection into the Park Falls config file.


Step 4 - Create the run directories
-----------------------------------

To set up the run directories, use a command like::

    gggutils i2s link-inp ./i2srun-config/i2s_parallel.cfg

This will take the given config file and create the run directories under the location specified by ``run_top_dir``.
They will be organized by site ID, then year, month, or day (depending on how split up they were in the config file).

Each run directory will have:

    * the flimit file *linked* to this directory
    * the I2S input file (as :file:`slice-i2s.in` or :file:`opus-i2s.in`). This will be a copy with the source, output,
      and flimit options changes to match the run directory structure.
    * a directory :file:`slices` or :file:`igms` with the slice date directories or interferograms linked
    * an empty directory, :file:`spectra` for the spectra to be generated in.

In the top run directory, there will also be created, by default, a file :file:`multii2s.sh` file. Analagously to
the :file:`multiggg.sh` file, this can be used with GNU parallel to run each day/month/year simultaneously.

Step 5 - Run using GNU Parallel
-------------------------------

Navigate to your top run directory, and find the :file:`multii2s.sh` file. This can be run using GNU parallel with::

    parallel -t --delay=1 -jN < multii2s.sh

replacing *N* with the number of processors you wish to use.