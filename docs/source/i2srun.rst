I2SRun - Utilities to run I2S in batch
======================================

Config file
-----------

Here is an example config file::

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