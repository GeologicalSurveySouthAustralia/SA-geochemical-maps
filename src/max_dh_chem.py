import pandas as pd
import numpy as np
from scipy.stats import zscore
from pathlib import Path


def norm_dh_chem(processed_data: pd.DataFrame, dh_data: str) -> pd.DataFrame:
    """Function to normalise and level the processed elemental geochemical data and
    return a dataframe

    Input:
    -----------
    processed_data: `pd.DataFrame` of processed and cleaned geochemical data from
                    `create_chem_dataset`
    dh_data: path to DH spatial data csv

    Returns:
    -----------
    `pd.DataFrame`: normlised and leveled geochemical data with mean, max and min
    element value, max, z_score_elemement value, plus DH spatial data.
    """

    dh_data = Path(dh_data)

    df_DH_data = pd.read_csv(dh_data)
    df = processed_data
    df["log_norm"] = df.groupby("CHEM_METHOD_CODE").converted_ppm.transform(
        lambda x: np.log(x)
    )
    df["Z_score_norm"] = df.groupby(["CHEM_METHOD_CODE"]).log_norm.transform(
        lambda x: zscore(x, ddof=1)
    )
    df = df[df.Z_score_norm.notnull()]

    df_ppm_grouped = df.groupby("DRILLHOLE_NUMBER").agg(
        {"converted_ppm": ["mean", "min", "max"]}
    )
    df_ppm_grouped.columns = [
        "converted_ppm_mean",
        "converted_ppm_min",
        "converted_ppm_max",
    ]
    df_ppm_grouped = df_ppm_grouped.reset_index()

    df_norm = df.groupby("DRILLHOLE_NUMBER").agg({"Z_score_norm": ["max"]})
    df_norm.columns = ["Z_score_max"]
    df_norm = df_norm.reset_index()

    df_normalised = df_ppm_grouped.merge(
        df_DH_data, how="left", left_on="DRILLHOLE_NUMBER", right_on="DRILLHOLE_NUMBER"
    )
    df_normalised = df_norm.merge(
        df_normalised,
        how="left",
        left_on="DRILLHOLE_NUMBER",
        right_on="DRILLHOLE_NUMBER",
    )

    return df_normalised


def max_dh_chem(processed_data: pd.DataFrame, dh_data: str) -> pd.DataFrame:
    """Function to aggregate the processed elemental geochemical data and
    return a dataframe containing max value in each drillhole

    Input:
    -----------
    processed_data: `pd.DataFrame` of processed and cleaned geochemical data from
                    `create_chem_dataset`
    dh_data: path to DH spatial data csv

    Returns:
    -----------
    `pd.DataFrame`: geochemical data with max element
    value, plus DH spatial data.
    """

    dh_data = Path(dh_data)

    df_DH_data = pd.read_csv(dh_data)
    df = processed_data

    df = df.loc[df.groupby(["DRILLHOLE_NUMBER"])["converted_ppm"].idxmax()]

    df_max = df.merge(
        df_DH_data, how="left", left_on="DRILLHOLE_NUMBER", right_on="DRILLHOLE_NUMBER"
    )

    return df_max


def max_dh_chem_interval(
    processed_data: pd.DataFrame, dh_data: str, interval: int
) -> pd.DataFrame:
    """Function to aggregate the processed elemental geochemical data and
    return a dataframe containing max value in each interval down hole for each
    drillhole

    Input:
    -----------
    processed_data: `pd.DataFrame` of processed and cleaned geochemical data from
                    `create_chem_dataset`
    dh_data: path to DH spatial data csv
    interval: aggregation interval down hole

    Returns:
    -----------
    `pd.DataFrame`: geochemical data with max element value for each interval,
    plus DH spatial data.
    """

    dh_data = Path(dh_data)

    df_DH_data = pd.read_csv(dh_data)
    df = processed_data

    # calculate median to-from depth
    df["median_depth"] = df[["DH_DEPTH_TO", "DH_DEPTH_FROM"]].apply(
        np.nanmedian, axis=1
    )

    end = df.median_depth.max().astype(int)
    # create bins to max depth and then bin median depths
    bins = pd.interval_range(start=0, end=end, freq=interval, closed="left")
    # labels = [f"{i} - {i + interval}" for i in range(0, end, interval)]

    df["bin"] = pd.cut(df["median_depth"], bins=bins)

    df.dropna(subset=["converted_ppm"], inplace=True)

    # aggregate max values over range
    grp = df.groupby(["DRILLHOLE_NUMBER", "bin"])
    df_max = df.loc[grp.converted_ppm.idxmax().dropna()]

    df_max = df_max.merge(
        df_DH_data, how="left", left_on="DRILLHOLE_NUMBER", right_on="DRILLHOLE_NUMBER"
    )

    return df_max


if __name__ == "__main__":
    norm_dh_chem()
    max_dh_chem()
    max_dh_chem_interval()
