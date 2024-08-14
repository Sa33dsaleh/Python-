import os
import pandas as pd
import geopandas as gpd

def process_files(file_dir, files, output_file):
    df1 = None
    
    for file in files:
        file_path = os.path.join(file_dir, file)
        df = pd.read_excel(file_path, 'Page 6')
        df = df.iloc[5:69, [0, 4]]  # Select rows and columns of interest
        df.columns = ['states', file[:5]]
        
        if df1 is None:
            df1 = df
        else:
            df1 = pd.concat([df1, df.iloc[:, 1]], axis=1)
    
    df1.to_excel(output_file, index=False)
    return df1

def load_and_prepare_data(file_path_2022, fips_file, census_file):
    df2022 = pd.read_excel(file_path_2022)
    fips = pd.read_csv(fips_file)
    census = pd.read_csv(census_file)
    
    df2022 = pd.merge(df2022, fips, left_on='states', right_on='stname')
    df2022[' st'] = df2022[' st'].apply(lambda x: '{0:0>2}'.format(x))
    
    df2022 = df2022.merge(census, left_on='stusps', right_on='Region')
    
    return df2022

def merge_geospatial_data(df2022, msa_shapefile, census_file):
    MSA_geo_raw = gpd.read_file(msa_shapefile)
    census = pd.read_csv(census_file)
    
    MSA_geo_raw = pd.merge(MSA_geo_raw, census, left_on="STATE_ABB", right_on='Region')
    MSA_geo_raw.rename(columns={'Zone_ID': 'CBSAFP2_ST'}, inplace=True)
    MSA_geo_raw['STATEFP'] = MSA_geo_raw['STATEFP'].astype(str).apply(lambda x: x.zfill(2))
    
    state_geo = MSA_geo_raw[['STATEFP', 'geometry']].dissolve(by='STATEFP', as_index=False)
    census_geo = MSA_geo_raw[['census_region', 'geometry']].dissolve(by='census_region', as_index=False)
    
    return state_geo, census_geo

def process_population_data(pop_file):
    pop_22 = pd.read_csv(pop_file)
    pop_22 = pop_22.iloc[[1],:].T.reset_index()
    pop_22[['state', 'new_column2']] = pop_22['index'].str.split('!!', expand=True)
    pop_22.columns = ['r', 'pop', 'state', 'e']
    pop_22 = pop_22.iloc[1:, [1, 2]]
    
    return pop_22

def calculate_metrics(df2022, pop_22):
    df2022['pop'] = df2022['pop'].str.replace(',', '').astype(int)
    df2022['year'] = df2022['year'].astype(int)
    
    df2022_c = df2022.groupby('census_region').agg(sum_points=('year', 'sum'), pop=('pop', 'sum')).reset_index()
    
    an_22 = df2022['year'].sum()
    bn_22 = df2022['pop'].sum()
    
    df2022_n = pd.DataFrame({
        'sum_points': [an_22],
        'pop': [bn_22]
    })
    
    df2022_c['vmtp'] = df2022_c['sum_points'] / df2022_c['pop'] * 1_000_000
    df2022_n['vmtp'] = df2022_n['sum_points'] / df2022_n['pop'] * 1_000_000
    df2022_n['vmtpy'] = df2022_n['vmtp'] / 396
    
    df2022['vmtp'] = df2022['year'] / df2022['pop'] * 1_000_000
    
    return df2022_c, df2022_n

def merge_geometries(state_geo, census_geo, df2022_c, df2022):
    df2022_ce = census_geo.merge(df2022_c, left_on='census_region', right_on='census_region')
    df2022_st = state_geo.merge(df2022, left_on='STATEFP', right_on=' st')
    
    df2022_ce['vmtpy'] = df2022_ce['vmtp'] / 396
    df2022_st['vmtpy'] = df2022_st['vmtp'] / 396
    
    return df2022_ce, df2022_st

# Example usage:
# file_dir = "path_to_directory"
# files = ["22aprtvt.xlsx", ...]  # List of files
# output_file = "result22.xlsx"
# df1 = process_files(file_dir, files, output_file)
# df2022 = load_and_prepare_data(output_file, "us-state-ansi-fips.csv", "census.csv")
# state_geo, census_geo = merge_geospatial_data(df2022, "NextGen_Zone_0825_shift_5070.shp", "census.csv")
# pop_22 = process_population_data("ACSDP1Y2021.DP05-2023-06-01T034545.csv")
# df2022_c, df2022_n = calculate_metrics(df2022, pop_22)
# df2022_ce, df2022_st = merge_geometries(state_geo, census_geo, df2022_c, df2022)
