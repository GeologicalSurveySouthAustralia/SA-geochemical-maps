#

import dask.dataframe as dd
import pandas as pd
from pathlib import Path


def create_dh_chem_data(
    path: str, element: str, method_path: str, out_path: str = None
) -> pd.DataFrame:
    """Function to create a 'clean' single element csv data set derived from the complete
    sarig_rs_chem_exp.csv. This isolates the element from the whole dh data set,
    converts BDL values to 1/2 the DL, drops rows that contain other symbols such as '>'
    and '-' and converts data to ppm. This data is used to create input data for
    processing.

    Input:
    -------------
    path: input path to main sarig_rs_chem_exp.csv input file
    out_path: default=path, path to place out_put files
    method_path: path to CSV containing mapped high level determination and digestion to lab method codes
    element: the particular element to extract and create the data set of, elements can be
    one of the following: ['U3O8','Au','SiO2','Al2O3','TiO2','FeO','MnO','MgO','CaO','Na2O',
    'K2O','P2O5','Fe2O3','Ba','Be','Ce','Dy','Er','Eu','Ga','Gd','Hf','Ho','La','Lu','Nd',
    'Pr','Rb','Sb','Sc','Sm','Sn','Sr','Ta','Tb','Th','Tm','U','W','Y','Yb','Zr','Nb','Ag',
    'As','Bi','Cd','Pb','Cu','Ge','Zn','Mn','Co','Cr','Cs','Li','Ni','V','LOI','B','Pd','In',
    'Mo','Se','Te','Tl','Ir','Pt','Rh','Ru','Ti','P','C','H2O_plus','H2O_minus','Ca','Al','Fe',
    'F','S','CO2','Mg','Hg','Os','K','V2O5','ThO2','WO3','Ta2O5','Nb2O5','Na','Br','Si','SO4','NaCl',
    'Cr2O3','GPSM','CO3','CaCO3','Insol','MgCO3','Cl','CaSO4','SO3','Re','Sr87_86','BaO','Total',
    'TOT/C','TOT/S','HMIN','SrO','CoO','NiO','ZnO','ZrO2','H2O','GoI']

    Returns:
    --------------
    csv: output data set of cleaned single element drill hole geochemical data
    `pd.DataFrame`: dataframe of cleaned geochemical data

    """
    path = Path(path)
    # load and filter to single element
    ddf = dd.read_csv(
        path,
        dtype={
            "ROCK_GROUP_CODE": "object",
            "ROCK_GROUP": "object",
            "LITHO_CODE": "object",
            "LITHO_CONF": "object",
            "LITHOLOGY_NAME": "object",
            "LITHO_MODIFIER": "object",
            "MAP_SYMBOL": "object",
            "STRAT_CONF": "object",
            "STRAT_NAME": "object",
            "COLLECTORS_NUMBER": "object",
            "COLLECTED_DATE": "object",
            "DH_NAME": "object",
            "OTHER_ANALYSIS_ID": "object",
            "LABORATORY": "object",
            "VALUE": "object",
            "CHEM_METHOD_CODE": "object",
            "CHEM_METHOD_DESC": "object",
        },
    )
    ddf = ddf.dropna(subset=["DRILLHOLE_NUMBER"])
    ddf = ddf[
        [
            "SAMPLE_NO",
            "SAMPLE_SOURCE_CODE",
            "DRILLHOLE_NUMBER",
            "DH_DEPTH_FROM",
            "DH_DEPTH_TO",
            "SAMPLE_ANALYSIS_NO",
            "ANALYSIS_TYPE_DESC",
            "LABORATORY",
            "CHEM_CODE",
            "VALUE",
            "UNIT",
            "CHEM_METHOD_CODE",
        ]
    ]
    ddf = ddf[ddf.UNIT != "cps"]
    df = ddf[ddf.CHEM_CODE == element].compute()

    # Clean single element dataset

    # df.drop(df[df.VALUE.str.contains(r'>', na=False, regex=False)].index, inplace=True)
    df.drop(df[df.VALUE.str.contains(r"-", na=False, regex=False)].index, inplace=True)
    # df['VALUE'] = df['VALUE'].astype(str).str.replace(">", "").astype(float) #remove '>' symb but keep data

    # create BDL/ODL flag and remove strings from values
    df["BDL"] = 0
    df.loc[df["VALUE"].str.contains("<", na=False, regex=False), "BDL"] = 1
    df.loc[df["VALUE"].str.contains(">", na=False, regex=False), "BDL"] = 2
    df["VALUE"] = (
        df["VALUE"].astype(str).str.replace(r"[<>]", "", regex=True).astype(float)
    )

    # create converted_ppm col and convert Oxides to Elements

    if element == "Fe2O3":
        df["VALUE"] = df["VALUE"] / 1.4297
    elif element == "FeO":
        df["VALUE"] = df["VALUE"] / 1.2865
    elif element == "U3O8":
        df["VALUE"] = df["VALUE"] / 1.1792
    elif element == "CoO":
        df["VALUE"] = df["VALUE"] / 1.2715
    elif element == "NiO":
        df["VALUE"] = df["VALUE"] / 1.2725
    else:
        pass

    df["converted_ppm"] = df["VALUE"]

    df.loc[(df["UNIT"] == "%"), "converted_ppm"] = (
        df.loc[(df["UNIT"] == "%"), "VALUE"] * 10000
    )

    df.loc[(df["UNIT"] == "ppb"), "converted_ppm"] = (
        df.loc[(df["UNIT"] == "ppb"), "VALUE"] / 10000
    )

    # convert the BDL values to low but non-zero values
    # df.loc[df['BDL'] == 1,'VALUE'] = df.loc[df['BDL'] == 1,'VALUE'] /2 this would convert to 1/2 reported DL
    if element == "Au" or element == "Ag":
        df.loc[
            df["BDL"] == 1, "converted_ppm"
        ] = 0.00001  # convert bdl values to 0.01ppb for Au and Ag
    else:
        df.loc[df["BDL"] == 1, "converted_ppm"] = 0.001

    df["CHEM_METHOD_CODE"].fillna(value="unk", inplace=True)
    df = df[~(df.VALUE == 0.0)]
    # print(f'The distribution of {element} is:')
    # print(df.describe())

    method_path = Path(method_path)
    chem_methods = pd.read_csv(method_path)

    determination_map = chem_methods.set_index("CHEM_METHOD")[
        "DETERMINATION_CODE_RD"
    ].to_dict()
    digestion_map = chem_methods.set_index("CHEM_METHOD")["DIGESTION_CODE_RD"].to_dict()
    fusion_map = chem_methods.set_index("CHEM_METHOD")["FUSION_TYPE"].to_dict()

    df["DETERMINATION"] = df.CHEM_METHOD_CODE.map(determination_map).fillna("unknown")
    df["DIGESTION"] = df.CHEM_METHOD_CODE.map(digestion_map).fillna("unknown")
    df["FUSION"] = df.CHEM_METHOD_CODE.map(fusion_map).fillna("unknown")

    if out_path is None:
        out_path = path
    else:
        out_path = Path(out_path)

    out_file = out_path / f"{element}_processed.csv"

    df.to_csv(out_file, index=False)

    return df


if __name__ == "__main__":
    create_dh_chem_data()
