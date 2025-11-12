# Module to reuse predifened plots
# TODO - implement savefig's with correct path

import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt  # to plot graph
import pandas as pd  # dataframe library
import numpy as np
import datetime as dt
import math


# From guid 22
def plt_violin_seaborn(
    df,
    xval,
    yval,
    title,
    ylabel,
    figsize=None,
    start_yymmdd=None,
    end_yymmdd=None,
    start_yymm=None,
    end_yymm=None,
    *argv,
    **kwargs,
):

    # Meta information
    for key, value in kwargs.items():
        print(key + " : " + value)

    # args for costum description
    for arg in argv:
        print(arg)

    font = {"weight": "bold", "size": 25}

    matplotlib.rc("font", **font)
    if not start_yymmdd and not end_yymmdd:
        # print('From: ',start_date_pick.value, 'to: ', end_date_pick.value)
        print()
    else:
        print("From: ", start_yymmdd, "to: ", end_yymmdd)

    if figsize:
        plt.figure(figsize=figsize)
    else:
        plt.figure()

    # Filter input date
    if start_yymmdd and end_yymmdd:
        df = df[df["YYMMDD"] <= int(end_yymmdd)]
        df = df[df["YYMMDD"] >= int(start_yymmdd)]
    if start_yymm and end_yymm:
        df = df[df["YYMM"] <= int(end_yymm)]
        df = df[df["YYMM"] >= int(start_yymm)]

    # https://www.graphpad.com/support/faq/violin-plots-and-logarithmic-axes/
    # Issue regarding negative values in violinplot
    # the "cut=0" truncates the data to the actual min and max values
    ax = sns.violinplot(x=df[xval], y=df[yval], cut=0)
    ax.set_title(title)
    plt.xticks(rotation=15, ha="right")
    # Append metainformation to the bottom of the chart (for export)
    ax.set_ylabel(ylabel)
    sns.despine(left=True)

    metainfo = get_metainfo_string(**kwargs)
    save_fig_in_subfolder(
        ax.figure,
        (
            "violin_" + yval + "_" + xval + metainfo + (start_yymm + "-" + end_yymm)
            if start_yymm is not None
            else (
                ("violin_" + yval + "_" + xval + metainfo + (start_yymmdd + "-" + end_yymmdd))
                if start_yymmdd is not None
                else "violin_" + yval + "_" + xval + metainfo + get_now_time_as_string()
            )
        ),
    )


def get_metainfo_string(**kwargs) -> str:
    result = ""
    for key, value in kwargs.items():
        value = value.replace("https://", "")
        result = result + "-" + value
    return result


def _distance(a, b):
    if a == b:
        return 0
    elif (a < 0) and (b < 0) or (a > 0) and (b > 0):
        if a < b:
            return abs(abs(a) - abs(b))
        else:
            return -(abs(abs(a) - abs(b)))
    else:
        return math.copysign((abs(a) + abs(b)), b)


# From guid 11a
def define_heat_map(
    zval,
    yval,
    xval,
    df,
    agg,
    colours,
    title,
    start_yymm=None,
    end_yymm=None,
    start_yymmdd=None,
    end_yymmdd=None,
    vmin=49.9,
    vmax=50.1,
    unit="",
    figsize=(25, 12),
    save=False,
):
    # limits
    hmin = 0
    hmax = 2359

    # if user date input: shorten data
    if start_yymm is not None and end_yymm is not None:
        df = df[df.YYMM >= int(start_yymm)]
        df = df[df.YYMM <= int(end_yymm)]
    if start_yymmdd is not None and end_yymmdd is not None:
        df = df[df.YYMMDD >= int(start_yymmdd)]
        df = df[df.YYMMDD <= int(end_yymmdd)]

    # create a pivot table to aggregate the data
    pivot_hm = pd.pivot_table(
        df,
        index=[yval],
        columns=[xval],
        values=[zval],
        # fill_value = 0,
        margins=False,
        aggfunc=[eval(agg)],
        dropna=True,  # keep null values
    )

    # Filter Data
    pivot_hm = pivot_hm[pivot_hm.index >= hmin]
    pivot_hm = pivot_hm[pivot_hm.index <= hmax]

    # Meta information
    """
    print('Tenant: ', url_widget.value)
    print('Source: ', wid_stream.value)
    print('Resolution: ', wid_reso.value)
    """
    if xval == "YYMM":
        print("Data from: ", df["YYMM"].iloc[0], "to: ", df["YYMM"].iloc[-1])
    elif xval == "YYMMDD":
        print()
        # print('Data from: ', df['YYMMDD'].iloc[0], 'to: ', df['YYMMDD'].iloc[-1])
    else:
        print("Data from: ", df["DHOD"].iloc[0], "to: ", df["DHOD"].iloc[-1])
    # create plot area
    fig, ax = plt.subplots(figsize=figsize)
    im = ax.imshow(
        pivot_hm,
        vmin=vmin,
        vmax=vmax,
        cmap=colours,
        origin="lower",  # desc y-axis values
        aspect="auto",
        # interpolation='bilinear'  #comment out for no-interpolation
    )

    # Y AXIS rows
    pr_hm = pivot_hm.shape[0]
    plt.yticks(np.arange(0, pr_hm), rotation=0)

    ax.set_yticklabels(pivot_hm.index, fontsize=18)

    # if to much yvalues shorten them
    if len(pivot_hm) > 200:
        temp = []
        for indx, item in enumerate(pivot_hm.index):
            if indx % 60 == 0:
                temp.append(item)
            else:
                temp.append(None)
        ax.set_yticklabels(temp, fontsize=18)
    ax.set_ylabel(yval, fontsize=18)

    # add helper lines

    # differ sample rate
    # for i in range(0,24):
    #    ax.axhline(i*constant, color='gray', linewidth=1 )
    diff = _distance(
        pivot_hm.shape[1], len(pivot_hm.columns.levels[2])
    )  # DIRTY FIX for mod difference (set_xticks and set_xticklabels) remove this if breaks

    # X AXIS columns
    pc_hm = pivot_hm.shape[1]  # number of
    plt.xticks(np.arange(0, pc_hm + diff), rotation=90)
    ax.set_xlabel(xval, fontsize=18)

    # Set costum tick labels TODO - change to 0.05 steps
    ax.set_xticklabels(pivot_hm.columns.levels[2], fontsize=18)
    if len(pivot_hm.columns.levels[2]) > 100:  # create new ticks if more than 20 columns (x-axis)
        nw_tick_list = []
        for i, item in enumerate(pivot_hm.columns.levels[2]):
            if xval == "YYMM" or xval == "DHOD":
                if i % 2 == 0:
                    nw_tick_list.append(item)
                else:
                    nw_tick_list.append(None)

            else:
                if i % 5 == 0:
                    nw_tick_list.append(item)
                else:
                    nw_tick_list.append(None)
        ax.set_xticklabels(nw_tick_list, fontsize=18)  # pivot_o.index)

    # define z-axis (colorbar)
    cbarlabel = zval + " " + unit
    cbar = ax.figure.colorbar(im, ax=ax)  # , **cbar_kw)
    cbar.ax.set_ylabel(cbarlabel, rotation=90, va="bottom", fontsize=18, labelpad=30)
    # cbar.set_label(zval, labelpad=20)

    # set labels
    # ax.grid(linewidth=1)
    fig.tight_layout()
    plt.xlabel(xval)
    plt.ylabel(yval)
    plt.title(title, fontsize=20)
    plt.tight_layout()
    plt.show()
    if save:
        save_fig_in_subfolder(
            fig, f"heatmap_{zval}_{get_now_time_as_string()}", "svg"
        )  # svg to avoid losing details on heatmaps
    # return pivot_hm #only if you are interested in pivot


# From Guid 1
def double_y_axis_plot(
    df,
    xval,
    yvals,
    yval_labels,
    ylabel,
    y2label=None,
    y2vals=None,
    y2val_labels=None,
    *argv,
    **kwargs,
):

    # Meta information
    for key, value in kwargs.items():
        print(key + " : " + value)

    # args for costum description
    for arg in argv:
        print(arg)

    fig = plt.figure()

    # Convert timestamps to datetime for xaxis
    x_time_ms = np.array(df[xval])
    x_time_s = np.divide(x_time_ms, 1000)
    len(x_time_s)
    x_time = [dt.datetime.fromtimestamp(ts) for ts in x_time_s]

    fig, ax1 = plt.subplots(figsize=(16, 7))
    x_label = "Date"
    ax1.set_xlabel(x_label)

    # set custom lable formatter for y-axis
    # from matplotlib.ticker import FuncFormatter
    # def kw(x, pos):
    #    return '%1.1f' % (x*1e-3)

    # formatter = FuncFormatter(kw)
    # ax1.yaxis.set_major_formatter(formatter)

    # plot left y-axis
    for i in range(len(yvals)):
        ax1.plot(x_time, df[yvals[i]], label=yval_labels[i])

    ax1.set_ylabel(ylabel)
    # ax1.set_ylim(0,1400)
    ax1.legend(frameon=True, loc="upper left", shadow=True)  # put axis legends in left corner

    # plot right y-axis
    if y2vals is not None:
        ax2 = ax1.twinx()  # merge ax2 to first plot
        ax2.set_ylabel(y2label)
        # Start with color cycle on end of ax1 (to not have duplicate colors)
        ax2._get_lines.prop_cycler = ax1._get_lines.prop_cycler

        for i in range(len(y2vals)):
            ax2.plot(x_time, df[y2vals[i]], label=y2val_labels[i])

        # ax2.set_ylim(-20,70)
        ax2.legend(frameon=True, loc="upper right", shadow=True)  # put axis legends in left corner

    # rotate x-ticks for better visability
    plt.setp(ax1.get_xticklabels(), rotation=15, horizontalalignment="right")

    fig.tight_layout()
    plt.legend()
    if y2label is not None:
        plt.title((ylabel + " vs " + x_label + " vs " + y2label), fontsize=25)
    else:
        plt.title((ylabel + " vs " + x_label), fontsize=25)
    plt.show()

    save_fig_in_subfolder(fig, "x_y_axis_plot" + get_now_time_as_string())


# From GUID 18-20
def pair_plot(df, variables, zval, *argv, **kwargs):

    # Meta information
    for key, value in kwargs.items():
        print(key + " : " + value)

    # args for costum description
    for arg in argv:
        print(arg)

    g = sns.pairplot(
        df,
        vars=variables,
        palette="hls",
        kind="scatter",
        markers="o",
        hue=zval,
        plot_kws=dict(s=50, edgecolor="black", linewidth=0.1),
    )
    metainfo = get_metainfo_string(**kwargs)

    save_fig_in_subfolder(g, "pair_plot_" + "_" + metainfo + get_now_time_as_string())


def get_list_as_string(li):
    res = ""
    for item in li:
        res += "_" + str(item)
    return res


def save_fig_in_subfolder(fig, fname, format=""):
    # TODO change path for linux
    save_dir = "export/figures/"
    mkdir_p(save_dir)

    if format != "":
        fig.savefig(save_dir + fname + f".{format}", format=format)
    else:
        fig.savefig(save_dir + fname + ".png")


def mkdir_p(mypath):
    """Creates a directory. equivalent to using mkdir -p on the command line"""

    from errno import EEXIST
    from os import makedirs, path

    try:
        makedirs(mypath)
    except OSError as exc:  # Python >2.5
        if exc.errno == EEXIST and path.isdir(mypath):
            pass
        else:
            raise


def get_now_time_as_string():

    utc_time = dt.datetime.utcnow()
    return utc_time.strftime("_%Y_%m_%d-%H_%M_%S")


def hist_with_cumulative(
    df: pd.DataFrame,
    column: str,
    divider: float = 1.0,
    bins: int = 16,
    figsize: tuple = (15, 8),
    x_label: str = "Gi",
    y1_label: str = "Count",
    y2_label: str = "Cumulative Sum",
    title: str = "Histogram and Cumulative Distribution",
) -> None:
    sns.set_style("whitegrid")
    adjusted_data = df[column] / divider
    weights = adjusted_data.values

    fig, ax1 = plt.subplots(figsize=figsize)

    ax1.hist(
        adjusted_data, weights=weights, bins=bins, edgecolor="black", alpha=0.7, label="Sum per bin"
    )
    ax1.set_xlabel(x_label)
    ax1.set_ylabel(y1_label, color="blue")
    ax1.tick_params(axis="y", labelcolor="blue")
    ax1.legend(loc="upper left")

    ax2 = ax1.twinx()
    ax2.hist(
        adjusted_data,
        weights=weights,
        bins=bins,
        edgecolor="black",
        alpha=0.7,
        cumulative=True,
        histtype="step",
        label="Cumulative sum",
        color="red",
    )
    ax2.set_ylabel(y2_label, color="red")
    ax2.tick_params(axis="y", labelcolor="red")
    ax2.legend(loc="upper right")

    plt.title(title)
    plt.show()
