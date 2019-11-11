from matplotlib import pyplot as plt
from pandas.plotting import register_matplotlib_converters
import re

from .. import target_analysis as tgts

register_matplotlib_converters()
plt.subplots_adjust()

def plot_comparison(matched_df, column, xraw=False, plot_type='diff', old_suffix='_old', new_suffix='_new', hlines=tuple(),
                    vlines=tuple(), suptitle=None, pdf=None, plot_kws=None):

    if plot_kws is None:
        plot_kws = dict()

    old_column = column + old_suffix
    new_column = column + new_suffix

    if plot_type == 'diff':
        fig, axs = _make_diff_plot(matched_df, old_column, new_column, xraw, **plot_kws)
    elif plot_type == 'hists':
        fig, axs = _make_hists(matched_df, old_column, new_column, xraw, **plot_kws)
    else:
        raise ValueError('Plot type "{}" not recognized'.format(plot_type))

    for ax in axs:
        for y in hlines:
            ax.axhline(y, color='k', linestyle='--')
        for x in vlines:
            ax.axvline(x, color='k', linestyle='--')

    plt.subplots_adjust(wspace=0.5)
    if suptitle:
        fig.suptitle(suptitle)

    if pdf is not None:
        pdf.savefig(fig, bbox_inches='tight')
        plt.close(fig)
    else:
        return fig, axs


def _get_column(df, colname, raw):
    if colname not in df.keys():
        return None

    if not raw:
        return df[colname]

    unit = re.search(r'pp[mbt]', colname)
    if unit is None:
        scale = 1
    else:
        unit_dict = {'ppm': 1e6, 'ppb': 1e9, 'ppt': 1e12}
        scale = unit_dict[unit.group()]

    return tgts.recalc_x(df, colname, scale)


def _make_diff_plot(matched_df, old_column, new_column, xraw, old_label='', new_label='', x_column='date_old',
                    xlabel='Date'):
    if not old_label.endswith(' '):
        old_label += ' '
    if not new_label.endswith(' '):
        new_label += ' '
    ylabel = '{oldlab}{oldcol} - {newlab}{newcol}'.format(oldlab=old_label, oldcol=old_column,
                                                          newlab=new_label, newcol=new_column)
    relylabel = '100% * ({oldlab}{oldcol} - {newlab}{newcol})/\n({newlab}{newcol})'.format(
        oldlab=old_label, oldcol=old_column, newlab=new_label, newcol=new_column
    )
    if xraw:
        ylabel += ' (raw)'
        relylabel += ' (raw)'

    with plt.rc_context({'font.size': 14}):
        fig, axs = plt.subplots(1, 2, figsize=(12,6))
        x = matched_df[x_column]
        y1 = _get_column(matched_df, old_column, raw=xraw)
        y2 = _get_column(matched_df, new_column, raw=xraw)
        if y1 is None or y2 is None:
            if y1 is None and y2 is None:
                missing = 'both'
            elif y1 is None:
                missing = old_column
            elif y2 is None:
                missing = new_column

            raise KeyError('Cannot plot difference of {} and {}, {} is/are missing'.format(old_column, new_column, missing))
        dy = y2 - y1
        rel_dy = 100 * dy / y1

        axs[0].plot(x, dy, linestyle='none', marker='.')
        axs[0].set_xlabel(xlabel)
        axs[0].set_ylabel(ylabel)
        for tick in axs[0].get_xticklabels():
            tick.set_rotation(45)

        axs[1].plot(x, rel_dy, linestyle='none', marker='.')
        axs[1].set_xlabel(xlabel)
        axs[1].set_ylabel(relylabel)
        for tick in axs[1].get_xticklabels():
            tick.set_rotation(45)

        return fig, axs


def _make_hists(matched_df, old_column, new_column, xraw, old_label='', new_label='', bins=30):
    if not old_label.endswith(' '):
        old_label += ' '
    if not new_label.endswith(' '):
        new_label += ' '
    xlabel1 = '{oldlab}{oldcol}'.format(oldlab=old_label, oldcol=old_column)
    xlabel2 = '{newlab}{newcol}'.format(newlab=new_label, newcol=new_column)
    if xraw:
        xlabel1 += ' (raw)'
        xlabel2 += ' (raw)'

    with plt.rc_context({'font.size': 14}):
        fig, axs = plt.subplots(1, 2, figsize=(12, 6))
        data1 = _get_column(matched_df, old_column, raw=xraw)
        data2 = _get_column(matched_df, new_column, raw=xraw)

        if data1 is not None:
            axs[0].hist(data1, bins=bins)
        axs[0].set_xlabel(xlabel1)
        for tick in axs[0].get_xticklabels():
            tick.set_rotation(45)
        
        if data2 is not None:
            axs[1].hist(data2, bins=bins)
        axs[1].set_xlabel(xlabel2)
        for tick in axs[1].get_xticklabels():
            tick.set_rotation(45)

    return fig, axs
