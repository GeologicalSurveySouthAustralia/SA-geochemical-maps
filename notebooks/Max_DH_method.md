## Process to extract and transform single element data to develop maximum downhole geochem data from SA Geodata.

### Required columns
- From the drill hole chem data tables we need the 'CHEM_CODE', 'VALUE', 'UNIT', 'DRILLHOLE_NUMBER', 'CHEM_METHOD_CODE'

### required elements
- 'Fe2O3','FeO, 'Fe', 'U', 'UsO8', 'Ag','Pb','Cu','Zn','Co', 'CoO','Ni','NiO', 'Au'

### selection
For each element required:
- SELECT ('CHEM_CODE', 'VALUE', 'UNIT', 'DRILLHOLE_NUMBER') FROM table WHERE UNIT != 'cps' AND CHEM_METHOD_CODE is in (method code list: See attached excel for method codes to include)
- This should select only the values we need to transform and select from.
- The 'VALUE' col will contain strings and floats.
- Need to drop any rows that contain '-'
- For this exercise we can also probably drop any below detection limit values as well, so 'VALUE' rows that contain '<'.
- Need to strip 'VALUE' rows that contain '>' and retain the value

### transformations
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

### select max value
Once all units are converted we need to select the max value from each drillhole.
- need to groupby['DRILLHOLE_NUMBER'] and select the Max 'VALUE', or Max 'times_average_crustal_abundance'

### display scale
- For display, need to present 'times_average_crustal_abundance data and all data need to be shown on a log scale except for Fe which needs to be on a linear scale.