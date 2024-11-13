from KMLExtractor_Helper import KMLDataExtractor, KMLMappings
import pandas as pd
import sqlite3
import os
from datetime import datetime
from pathlib import Path

# Create the /data directory if it does not exist
base_path = Path('./data')
base_path.mkdir(parents=True, exist_ok=True)

# 1. GENERATING RENT AND SALE OFFERS DATABASE (1 database)
# 1.1 Data Extraction step:
print(f'------------------------- DATA ETL PIPELINE STARTED --------------------------------')
print(f"---- Extracting data for sale and rent offers from 2011 to 2021 -> [1/3] started ---")
sales_mappings = KMLMappings.sales_year_mappings
rents_mappings = KMLMappings.rents_year_mappings


sales_urls = {
    2011: "https://www.google.com/maps/d/kml?mid=1o-MfPNEPgt7FFjuk7bR1WC8DGN5mwkgf&resourcekey&forcekml=1",
    2012: "https://www.google.com/maps/d/kml?mid=1Vqq1_g9nCJ969sN-v4S7RNRkCXXTUK0r&resourcekey&forcekml=1",
    2013: "https://www.google.com/maps/d/kml?mid=14FYOHIyYMj365G1mMuGk0Az2YqncYySZ&resourcekey&forcekml=1",
    2014: "https://www.google.com/maps/d/kml?mid=1uBZjSi53_njkmAvXlVr2Q6Ynlt04s15i&resourcekey&forcekml=1",
    2015: "https://www.google.com/maps/d/kml?mid=1t1QNWWZjkvRKG0zEtjNRILNzGfrk896M&resourcekey&forcekml=1",
    2016: "https://www.google.com/maps/d/kml?mid=1Vmf5hKsaFQlo94BNYZ5vv5cattIIipq8&resourcekey&forcekml=1",
    2017: "https://www.google.com/maps/d/kml?mid=1ImJDRhXErEbezl5PXilxV3FXyNovW0Rb&resourcekey&forcekml=1",
    2018: "https://www.google.com/maps/d/kml?mid=1T9jpU6erir832dc2X_ljBgHOhveE3Zwy&resourcekey&forcekml=1",
    2019: "https://www.google.com/maps/d/kml?mid=1YVqcLo3KcaN9Ujou77FKqyhpOhy92fg&resourcekey&forcekml=1",
    2020: "https://www.google.com/maps/d/kml?mid=1X1bAtSD5S1M0fxif3RWBNz-ju2q6HfU&resourcekey&forcekml=1",
    2021: "https://www.google.com/maps/d/kml?mid=1dvXgm6Xb_hHjsVqhh6FWcZuq1g1pjTI&resourcekey&forcekml=1"
}

rents_urls = {
    2011: "https://www.google.com/maps/d/kml?mid=1hx3Ita6dQP3XhOs4H_-bqLxPgeAS76hQ&resourcekey&forcekml=1",
    2012: "https://www.google.com/maps/d/kml?mid=1i14McURm1oNP1HsZ9TxuMgQcOf5xndl2&resourcekey&forcekml=1",
    2013: "https://www.google.com/maps/d/kml?mid=11OVliCLwxfpuT4M0M1TREbqGFwN5XB6C&resourcekey&forcekml=1",
    2014: "https://www.google.com/maps/d/kml?mid=1NzdDw2en09GQGYpZCM1B9EDDTOOOYzeb&resourcekey&forcekml=1",
    2015: "https://www.google.com/maps/d/kml?mid=1VjJslKQ9xtXJHHQ9PYew_gDvy0kovaDS&resourcekey&forcekml=1",
    2016: "https://www.google.com/maps/d/kml?mid=1okB4ruto0NlDy-sYKdGg5py8zLn0U-QM&resourcekey&forcekml=1",
    2017: "https://www.google.com/maps/d/kml?mid=1pQVhOAY7_5XMLgDOISpgI1hZrS5vToMF&resourcekey&forcekml=1",
    2018: "https://www.google.com/maps/d/kml?mid=1lRnic0sQSU_BpdtcPOBJ4UD3ctgk8hSp&resourcekey&forcekml=1",
    2019: "https://www.google.com/maps/d/kml?mid=1y6Gj9EvlMyRfdSRjc21tWJEEa41gr9E&resourcekey&forcekml=1",
    2020: "https://www.google.com/maps/d/kml?mid=1iMEcsfAfac13-MwCDJdCCoazDbQQhvQ&resourcekey&forcekml=1",
    2021: "https://www.google.com/maps/d/kml?mid=1RJNbIHsnWIcaS4uyb4hCCGjGypME59M&resourcekey&forcekml=1"
}
# Process sales and rents: Extract KML from each year's dataset and turn into CSV files
sales_extractor = KMLDataExtractor(sales_mappings)
rents_extractor = KMLDataExtractor(rents_mappings)

# Integration of all years (2011-2021) into two separate datasets for sales and rents
sales_data = sales_extractor.process_multiple_years(sales_urls) 
rents_data = rents_extractor.process_multiple_years(rents_urls)

if not sales_data.empty and not rents_data.empty:
    rents_sales_unified_df = pd.concat([sales_data, rents_data], ignore_index=True)
    
    # 1.2. Data Transformation step:
    # Filter rows where 'Predio' starts with 'APARTAMENTO' or 'CASA'
    rents_sales_df = rents_sales_unified_df[rents_sales_unified_df['Predio'].str.startswith(('APARTAMENTO', 'CASA'), na=False)].copy()
    
    # Convert 'Fecha' to 'YYYY.MM' format
    def format_fecha(fecha):
        try:
            date_obj = datetime.strptime(fecha, '%d-%m-%Y')
        except ValueError:
            try:
                date_obj = datetime.strptime(fecha, '%d/%m/%Y')
            except ValueError:
                return fecha  # Leave as-is if date parsing fails
        return date_obj.strftime('%Y.%m')

    rents_sales_df['Fecha'] = rents_sales_df['Fecha'].apply(format_fecha)
    
    # Process "Valor Comercial" based on inconsistencies in the dataset with the values
    def clean_valor_comercial(valor, valor_m2):
        if pd.isna(valor):
            return None
        # Case 1: Valor M2 is empty, commas as decimal points, periods as thousand separators
        if pd.isna(valor_m2):
            valor = valor.replace('.', '').replace(',', '.')
        # Case 2: Valor M2 is not empty, commas as thousand separators
        elif ',' in valor:
            valor = valor.replace(',', '')
        # Case 3: No commas or periods, treat as integer
        return float(valor)
    
    # Apply the cleaning function to "Valor Comercial"
    rents_sales_df['Valor Comercial'] = rents_sales_df.apply(lambda row: clean_valor_comercial(row['Valor Comercial'], row['Valor M2']), axis=1)
    
    # Convert 'Area Privada' and 'Valor M2' columns to numeric, coercing errors
    rents_sales_df['Area Privada'] = pd.to_numeric(rents_sales_df['Area Privada'], errors='coerce')
    rents_sales_df['Valor M2'] = pd.to_numeric(rents_sales_df['Valor M2'], errors='coerce')
    
    # Fill missing 'Valor M2' values with the result of 'Valor Comercial' / 'Area Privada', only where feasible
    rents_sales_df['Valor M2'] = rents_sales_df.apply(
        lambda row: row['Valor Comercial'] / row['Area Privada'] if pd.isna(row['Valor M2']) and row['Area Privada'] > 0 else row['Valor M2'], 
        axis=1
    )
    
    # 1.3 Data loading step:
    database_path_1 = base_path / 'sales_rents_2011_2021.sqlite'
    with sqlite3.connect(database_path_1) as conn:
        rents_sales_df.to_sql('sales_rents', conn, index=False, if_exists='replace')
    
    print(f"------- Successfully generated sales_rents_2011_2021.sqlite -> [1/3] done ---")
    print(f'------------------------------------------------------------------------------\n')
else:
    print("No data available to save in the unified file.")


# 2. GENERATING TOURISM DATABASES (2 databases):
# 2.1 DATABASE # 1:
# Step 1: Extraction
print(f"------- Extracting data for monthly entry of Colombians and foreigners -> [2/3] started ---")
# Load the CSV file from the provided path
file_path = 'https://medata.gov.co/sites/default/files/distribution/1-010-04-000188/ingreso_mensual_de_extranjeros_y_colombianos_por_punto_migratorio_jose_maria_cordova.csv'
monthly_col_for = pd.read_csv(file_path) # Monthly entry of Colombians and foreigners through airport

# Step 2: Transformation
# Delete the 'ing_indic' column
monthly_col_for.drop(columns=['ing_indic'], inplace=True, errors='ignore')

# Rename columns
monthly_col_for.rename(columns={
    'ing_nacionalidad': 'Nationality',
    'ing_periodo': 'Period',
    'ing_valor': 'Number'
}, inplace=True)

# Format the 'Period' column from "YYYYMM" to "YYYY.MM"
monthly_col_for['Period'] = monthly_col_for['Period'].astype(str).apply(lambda x: f"{x[:4]}.{x[4:]}")

# Delete rows where 'Period' is below 2011
# Convert 'Period' to float for comparison and filter out values below 2011
monthly_col_for['Period_numeric'] = monthly_col_for['Period'].apply(lambda x: float(x[:4])) # take the year out of the Period
monthly_col_for = monthly_col_for[monthly_col_for['Period_numeric'] >= 2011]
monthly_col_for.drop(columns=['Period_numeric'], inplace=True)

# Step 3: Loading
# Save the transformed DataFrame to an SQLite database
database_path_2 = base_path / 'monthly_entry_colombians_foreigners.sqlite'
with sqlite3.connect(database_path_2) as conn:
    monthly_col_for.to_sql('monthly_entry', conn, index=False, if_exists='replace')

print(f"------- Successfully generated monthly_entry_colombians_foreigners.sqlite -> [2/3] done ---")
print(f'-----------------------------------------------------------------------------------------\n')

# 2.2 DATABASE # 2:
print(f"--- Extracting data for monthly entry passanger national and international origin -> [3/3] started ---")
# Step 1: Extraction
# Load both CSV files
foreigners_country_origin_url = 'https://medata.gov.co/sites/default/files/distribution/1-010-04-000194/llegada_mensual_de_extranjeros_por_pais_de_residencia_por_punto_migratorio.csv'
colombians_city_origin_url = 'https://medata.gov.co/sites/default/files/distribution/1-010-04-000196/llegada_pasajeros_mensual_por_aeropuerto_de_origen_nacional.csv'

foreigners_country_origin_df = pd.read_csv(foreigners_country_origin_url)
colombians_city_origin_df = pd.read_csv(colombians_city_origin_url)

# Step 2: Transformation
# 2.2.1 Transformation for the first CSV (foreigners data)
# Delete 'lle_indicador' column
foreigners_country_origin_df.drop(columns=['lle_indicador'], inplace=True, errors='ignore')

# Rename columns
foreigners_country_origin_df.rename(columns={
    'lle_codigo': 'Code',
    'lle_origenpax': 'Origin',
    'lle_periodo': 'Period',
    'lle_valor': 'Number'
}, inplace=True)

# Create 'Nationality' column with value "Extranjero"
foreigners_country_origin_df['Nationality'] = "Extranjero"

# Format 'Period' column from "YYYYMM" to "YYYY.MM"
foreigners_country_origin_df['Period'] = foreigners_country_origin_df['Period'].astype(str).apply(lambda x: f"{x[:4]}.{x[4:]}")

# Delete rows where 'Period' is below 2011
foreigners_country_origin_df['Period_numeric'] = foreigners_country_origin_df['Period'].apply(lambda x: float(x[:4]))
foreigners_country_origin_df = foreigners_country_origin_df[foreigners_country_origin_df['Period_numeric'] >= 2011]
foreigners_country_origin_df.drop(columns=['Period_numeric'], inplace=True)

# 2.2.2 Transformation for the second CSV (Colombians data)
# Delete 'lle_indicador' column
colombians_city_origin_df.drop(columns=['lle_indicador'], inplace=True, errors='ignore')

# Rename columns
colombians_city_origin_df.rename(columns={
    'lle_codigo': 'Code',
    'lle_llegadanal': 'Origin',
    'lle_periodo': 'Period',
    'lle_valor': 'Number'
}, inplace=True)

# Create 'Nationality' column with value "Colombiano"
colombians_city_origin_df['Nationality'] = "Colombiano"

# Format 'Period' column from "YYYYMM" to "YYYY.MM"
colombians_city_origin_df['Period'] = colombians_city_origin_df['Period'].astype(str).apply(lambda x: f"{x[:4]}.{x[4:]}")

# Delete rows where 'Period' is below 2011
colombians_city_origin_df['Period_numeric'] = colombians_city_origin_df['Period'].apply(lambda x: float(x[:4]))
colombians_city_origin_df = colombians_city_origin_df[colombians_city_origin_df['Period_numeric'] >= 2011] # Temporal colum to filter period
colombians_city_origin_df.drop(columns=['Period_numeric'], inplace=True)

# Replace all values in 'Code' column with "CB": Colombia for the second dataframe
colombians_city_origin_df['Code'] = "CB"

# Combine both transformed DataFrames into one
monthly_passengers_origin = pd.concat([foreigners_country_origin_df, colombians_city_origin_df], ignore_index=True)

# Step 3: Loading
# Save the combined DataFrame to an SQLite database
database_path_3 = base_path / 'monthly_passengers_origin.sqlite'
with sqlite3.connect(database_path_3) as conn:
    monthly_passengers_origin.to_sql('monthly_passengers', conn, index=False, if_exists='replace')
    
print(f"------- Successfully generated monthly_passengers_origin.sqlite -> [3/3] done ---")
print(f'----------------------------------------------------------------------------------\n')
print(f'---------------------- DATA ETL PIPELINE FINISHED ---------------------------------')