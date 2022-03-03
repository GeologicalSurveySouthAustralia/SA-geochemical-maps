South Australian geochemical maps
==============================

The Geological Survey of South Australia's SA Geodata database (back end to SARIG: https://map.sarig.sa.gov.au/) is the primary repository for geoscientific information in South Australia and contains data collected: from research and fieldwork conducted by GSSA staff; by mineral exploration companies who are required to submit most of the data they collect during their exploration programs to the state government, and; data collected by research institutions from analysis of core and rock samples held in the South Australian Core Library.  

The GSSA is always reviewing the types of data and datasets we provide and the potential derived products from this. The GSSA will soon be delivering a series of maximum down hole element datasets via SARIG.

This repository contains a series of notebooks used in the initial review of how to deliver such a dataset. The notebooks go through a data cleansing and data review process to identify spurious data and either exclude from the data package or to be reviewed and cleaned in the SA Geodata database.

The '1_SARIG_chem_load_clean.ipynb' notebook outlines the steps taken to do the data load and clean process, using copper as an example. This notebook we also outlines the aggregation process to determine the maximum value for each drill hole. The '2_Max_DH_Chem_lab mapping_EDA.ipynb' notebook then undertakes further exploratory data analysis (EDA) on the output datasets to review the actual data values in them and exclude some if required. Here we explore the various chemical methods used to determine the analyte values and review outliers by going back to the source data where possible. These cleaned datasets are then used to plot maps of the maximum down hole geochemical values for each of the elements Ag, Au, Co, Cu, Fe, Li, Ni, Pb, U and Zn across the state in the various 'Max_DH_element.ipynb' notebooks.

This repository also contains a mapping file to convert the lab specific analysis type codes to a generic determination, where it has been possible to find what the lab codes mean.

This entire process has also been automated into a command line program and python library called [pygeochemtools](https://pygeochemtools.readthedocs.io/en/latest/) available on [pypi](https://pypi.org/project/pygeochemtools/). 

**Note** The SARIG Data Package is an extract from the Geological Survey of South Australia’s (GSSA) geoscientific database SA Geodata. SA Geodata is the primary repository for geoscientific information in South Australia and contains data collected: from research and fieldwork conducted by GSSA staff; by mineral exploration companies who are required to submit most of the data they collect during their exploration programs to the state government, and; data collected by research institutions from analysis of core and rock samples held in the South Australian Core Library. This snapshot of the database was provided for the ExploreSA: Gawler Challenge and is valid as at Feburary 2020.

To run the notebooks locally
------------
* Git clone the repository
* Setup a virtualenv using conda or venv running Python 3.8
* `pip3 install -r requirements.txt`, or use conda install 

*note* Conda may be the easiest option as Cartopy has a number of dependencies, which come pre-packaged with the condaforge installation of Cartopy 


Project Organization
------------

    ├── README.md
    ├── requirements.txt  
    ├── notebooks
    │   ├── 1_SARIG_chem_load_clean.ipynb
    │   ├── 2_Max_DH_Chem_lab mapping_EDA.ipynb
    │   ├── Max_DH_Ag.ipynb
    │   ├── Max_DH_Au.ipynb
    │   ├── Max_DH_Co.ipynb
    │   ├── Max_DH_Cu.ipynb
    │   ├── Max_DH_Fe.ipynb
    │   ├── Max_DH_Li.ipynb
    │   ├── Max_DH_Ni.ipynb
    │   ├── Max_DH_Pb.ipynb
    │   ├── Max_DH_U.ipynb
    │   ├── Max_DH_Zn.ipynb
    │   └── chem_method_code_map.csv
    └── src                                         <--- Scripts to automate the cleaning and plotting process
        ├── create_chem_dataset.py              
        ├── interpolation.py   
        └── max_dh_chem.py

![Maximum down hole copper values](Cu_point_interval.gif)

Process summary
------------
Process to extract and transform single element data to develop maximum down hole geochem data from SA Geodata.

#### Required columns
- From the drill hole chem data tables we need the 'CHEM_CODE', 'VALUE', 'UNIT', 'DRILLHOLE_NUMBER', 'CHEM_METHOD_CODE'

#### Required elements
- 'Fe2O3','FeO, 'Fe', 'U', 'UsO8', 'Ag','Pb','Cu','Zn','Co', 'CoO','Ni','NiO', 'Au'

#### Selection
For each element required:
- SELECT ('CHEM_CODE', 'VALUE', 'UNIT', 'DRILLHOLE_NUMBER') FROM table WHERE UNIT != 'cps' AND CHEM_METHOD_CODE is not in (method code list: methods to exclude)
- This should select only the values we need to transform and select from.
- The 'VALUE' col will contain strings and floats.
- Need to drop any rows that contain '-'
- Convert any below detection limit values, so 'VALUE' rows that contain '<'.
- Need to strip 'VALUE' rows that contain '>' and '<' and retain the value

#### Transformations
For any elements that are oxides (Fe2O3, FeO etc) we need to convert to elemental values:
- 'Fe2O3':['VALUE'] / 1.4297
- 'FeO':['VALUE'] / 1.2865
- 'U3O8':['VALUE'] / 1.1792
- 'CoO'['VALUE'] / 1.2715
- 'NiO':['VALUE'] / 1.2725

Then all values need to be converted to ppm:
- if 'UNIT'=='%': ['VALUE'] * 10000
- if 'UNIT'=='ppb': ['VALUE'] / 10000

Then normalise 'VALUE' to 'times_average_crustal_abundance':
- 'Fe':['VALUE] / 67100
- 'Ni':['VALUE] / 59
- 'Ag':['VALUE] / 0.056
- 'Au':['VALUE] / 0.0013
- 'Co':['VALUE] / 26.6
- 'Cu':['VALUE] / 27
- 'Pb':['VALUE] / 11
- 'U':['VALUE] / 1.3
- 'Zn':['VALUE] / 72

#### Select max value
Once all units are converted we need to select the max value from each drill hole.
- need to groupby['DRILLHOLE_NUMBER'] and select the Max 'VALUE', or Max 'times_average_crustal_abundance'

#### Display scale
- For display, need to present 'times_average_crustal_abundance' data and all data need to be shown on a log scale except for Fe which needs to be on a linear scale.
    
--------
