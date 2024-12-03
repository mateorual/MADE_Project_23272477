from KMLExtractor_Helper import KMLDataExtractor, KMLMappings
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime

class Pipeline:
    """
        A data processing pipeline for extracting, transforming, and loading data from multiple sources 
        related to housing and tourism into a SQLite database. The pipeline integrates sales, rents, 
        and tourism data, applying transformations to standardize and clean the data before saving it.

        Attributes:
            base_path (Path): Directory path for data storage.
            database_name (Path): Path to the SQLite database file.
            sales_urls (dict): URLs for KML files containing sales data per year.
            rents_urls (dict): URLs for KML files containing rent data per year.
            sales_extractor (KMLDataExtractor): Extractor for sales data using year mappings.
            rents_extractor (KMLDataExtractor): Extractor for rents data using year mappings.
            entry_colombians_foreigners_url (str): URL for monthly entry data of Colombians and foreigners.
            foreigners_country_origin_url (str): URL for data on foreigners by country of origin.
            colombians_city_origin_url (str): URL for data on Colombians by city of origin.
        """
        
    def __init__(self):
        self.base_path = Path('../data') # Target directory
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.database_name = self.base_path / 'Housing_Tourism_Data.sqlite'
        self.sales_urls = {
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
        self.rents_urls = {
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
        self.sales_extractor = KMLDataExtractor(KMLMappings.sales_year_mappings)
        self.rents_extractor = KMLDataExtractor(KMLMappings.rents_year_mappings)
        self.entry_colombians_foreigners_url = 'https://medata.gov.co/sites/default/files/distribution/1-010-04-000188/ingreso_mensual_de_extranjeros_y_colombianos_por_punto_migratorio_jose_maria_cordova.csv'
        self.foreigners_country_origin_url = 'https://medata.gov.co/sites/default/files/distribution/1-010-04-000194/llegada_mensual_de_extranjeros_por_pais_de_residencia_por_punto_migratorio.csv'
        self.colombians_city_origin_url = 'https://medata.gov.co/sites/default/files/distribution/1-010-04-000196/llegada_pasajeros_mensual_por_aeropuerto_de_origen_nacional.csv'

    
    def _download_csv(self, url, retries=3, timeout=10):
        """Download CSV file with retry logic."""
        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=timeout)
                response.raise_for_status()
                return pd.read_csv(StringIO(response.text))
            except requests.exceptions.RequestException as e:
                print(f"Attempt {attempt + 1}/{retries} failed for URL: {url} | Error: {e}")
                if attempt < retries - 1:
                    continue
                else:
                    print(f"Failed to download after {retries} attempts. Skipping URL: {url}")
                    return pd.DataFrame()  # Return an empty DataFrame if all attempts fail
                
                
    def extract_data(self):
        """
        Extracts data from multiple sources, including sales and rent KML files, and CSV files 
        for tourism data. Downloads and processes these datasets into structured DataFrames.

        Returns:
            dict: A dictionary containing extracted datasets:
                - "sales_data": DataFrame for sales data.
                - "rents_data": DataFrame for rents data.
                - "tourism_1": DataFrame for monthly entries of Colombians and foreigners.
                - "foreigners": DataFrame for foreigners by country of origin.
                - "colombians": DataFrame for Colombians by city of origin.
        """
        
        print("Extracting data from all sources...")
        # Extract sales and rents data
        sales_data = self.sales_extractor.process_multiple_years(self.sales_urls)
        rents_data = self.rents_extractor.process_multiple_years(self.rents_urls)

        # Extract tourism datasets
        tourism_1 = pd.read_csv(self.entry_colombians_foreigners_url)
        foreigners = pd.read_csv(self.foreigners_country_origin_url)
        colombians = pd.read_csv(self.colombians_city_origin_url)

        print("[SUCCESS] Data extraction completed [1/3]")
        print("------------------------------------------------------------\n")
        return {
            "sales_data": sales_data,
            "rents_data": rents_data,
            "tourism_1": tourism_1,
            "foreigners": foreigners,
            "colombians": colombians
        }

    
    def transform_data(self, data):
        """
        Transforms the extracted datasets by cleaning, standardizing, and applying domain-specific 
        transformations. Combines and prepares data for further analysis and loading.

        Args:
            data (dict): A dictionary containing extracted datasets.

        Returns:
            dict: A dictionary containing transformed datasets:
                - "sales_rents": Combined and cleaned sales and rents data.
                - "tourism_1": Transformed data for monthly entries of Colombians and foreigners.
                - "tourism_2": Combined and cleaned data for monthly passengers with city/country of origin.
        """
        
        print("Transforming all datasets...")
        sales_rents_data = self._transform_sales_rents_data(data["sales_data"], data["rents_data"])
        tourism_data_1 = self._transform_tourism_data_1(data["tourism_1"])
        tourism_data_2 = self._transform_tourism_data_2(data["foreigners"], data["colombians"])

        print("[SUCCESS] Data transformation completed [2/3]")
        print("------------------------------------------------------------\n")
        return {
            "sales_rents": sales_rents_data,
            "tourism_1": tourism_data_1,
            "tourism_2": tourism_data_2
        }

    def _transform_sales_rents_data(self, sales_data, rents_data):
        if sales_data.empty or rents_data.empty:
            print("No sales or rents data to transform.")
            return None

        unified_data = pd.concat([sales_data, rents_data], ignore_index=True)

        # Filtering rows where 'Predio' starts with specific keywords
        filtered_data = unified_data[unified_data['Predio'].str.startswith(('APARTAMENTO', 'CASA'), na=False)].copy()

        # Formatting the 'Fecha' column
        filtered_data['Fecha'] = filtered_data['Fecha'].apply(self._format_fecha)

        # Cleaning 'Valor Comercial'
        filtered_data['Valor Comercial'] = filtered_data.apply(
            lambda row: self._clean_valor_comercial(row['Valor Comercial'], row['Valor M2']), axis=1)

        # Converting columns to numeric
        filtered_data['Area Privada'] = pd.to_numeric(filtered_data['Area Privada'], errors='coerce')
        filtered_data['Valor M2'] = pd.to_numeric(filtered_data['Valor M2'], errors='coerce')
        filtered_data['Area Lote'] = pd.to_numeric(filtered_data['Area Lote'], errors='coerce')

        # Filling missing 'Valor M2' values
        filtered_data['Valor M2'] = filtered_data.apply(
            lambda row: row['Valor Comercial'] / row['Area Privada'] if pd.isna(row['Valor M2']) and row['Area Privada'] > 0 else row['Valor M2'],
            axis=1
        )
        
        # Round 'Valor Comercial', 'Area Lote' and 'Valor M2' to integers
        filtered_data['Valor Comercial'] = filtered_data['Valor Comercial'].round().astype('Int64')
        filtered_data['Valor M2'] = filtered_data['Valor M2'].round().astype('Int64')
        filtered_data['Area Lote'] = filtered_data['Area Lote'].fillna(0).round().astype('Int32')
        
        # Rename column headers from Spanish to English
        filtered_data.rename(columns={
            "Fecha": "Period",
            "Investigacion": "Research",
            "Predio": "Property",
            "Estado": "Condition",
            "Barrio": "Neighborhood",
            "Estrato": "Stratum",
            "Area Privada": "Private_Area_m2",
            "Area Lote": "Lot_Area_m2",
            "Valor Comercial": "Commercial_Price_COP",
            "Valor M2": "Price_per_m2_COP",
            "Longitude": "Longitude",
            "Latitude": "Latitude"
        }, inplace=True)
        
        
        return filtered_data

    def _transform_tourism_data_1(self, tourism_1):
        # Dropping and renaming columns
        tourism_1.drop(columns=['ing_indic'], inplace=True, errors='ignore')
        tourism_1.rename(columns={
            'ing_nacionalidad': 'Nationality',
            'ing_periodo': 'Period',
            'ing_valor': 'Number'
        }, inplace=True)

        # Formatting and filtering
        tourism_1['Period'] = tourism_1['Period'].astype(str).apply(lambda x: f"{x[:4]}.{x[4:]}")
        tourism_1['Period_numeric'] = tourism_1['Period'].apply(lambda x: float(x[:4]))
        tourism_1 = tourism_1[tourism_1['Period_numeric'] >= 2011].drop(columns=['Period_numeric'])

        return tourism_1

    def _transform_tourism_data_2(self, foreigners, colombians):
        # Transform foreigners data
        foreigners.drop(columns=['lle_indicador'], inplace=True, errors='ignore')
        foreigners.rename(columns={
            'lle_codigo': 'Code',
            'lle_origenpax': 'Origin',
            'lle_periodo': 'Period',
            'lle_valor': 'Number'
        }, inplace=True)
        foreigners['Nationality'] = "Extranjero"
        foreigners['Period'] = foreigners['Period'].astype(str).apply(lambda x: f"{x[:4]}.{x[4:]}")
        foreigners['Period_numeric'] = foreigners['Period'].apply(lambda x: float(x[:4]))
        foreigners = foreigners[foreigners['Period_numeric'] >= 2011].drop(columns=['Period_numeric'])

        # Transform Colombians data
        colombians.drop(columns=['lle_indicador'], inplace=True, errors='ignore')
        colombians.rename(columns={
            'lle_codigo': 'Code',
            'lle_llegadanal': 'Origin',
            'lle_periodo': 'Period',
            'lle_valor': 'Number'
        }, inplace=True)
        colombians['Nationality'] = "Colombiano"
        colombians['Period'] = colombians['Period'].astype(str).apply(lambda x: f"{x[:4]}.{x[4:]}")
        colombians['Period_numeric'] = colombians['Period'].apply(lambda x: float(x[:4]))
        colombians = colombians[colombians['Period_numeric'] >= 2011].drop(columns=['Period_numeric'])
        colombians['Code'] = "CO"

        # Combine both datasets
        combined_data = pd.concat([foreigners, colombians], ignore_index=True)   
        # Delete rows with specific values in the "Origin" column
        combined_data = combined_data[~combined_data['Origin'].isin(["Acuerdo internacional", "Inconsistencia"])]
        # Delete rows with "Number" < 0 (passengers is always positive quantity)
        combined_data = combined_data[combined_data['Number'] >= 0]
        # Delete rows where "Code" does not match exactly two capital letters
        combined_data['Code'] = combined_data['Code'].astype(str)
        combined_data = combined_data[combined_data['Code'].str.match(r'^[A-Z]{2}$')]
        
        return combined_data

    @staticmethod
    def _format_fecha(fecha):
        try:
            date_obj = datetime.strptime(fecha, '%d-%m-%Y')
        except ValueError:
            try:
                date_obj = datetime.strptime(fecha, '%d/%m/%Y')
            except ValueError:
                return fecha
        return date_obj.strftime('%Y.%m')

    @staticmethod
    def _clean_valor_comercial(valor, valor_m2):
        if pd.isna(valor):
            return None
        if pd.isna(valor_m2):
            valor = valor.replace('.', '').replace(',', '.')
        elif ',' in valor:
            valor = valor.replace(',', '')
        return float(valor)

    def save_data_to_sqlite(self, data):
        """
        Saves the transformed datasets to an SQLite database. Each dataset is stored as a separate 
        table, with existing tables being replaced.

        Args:
            data (dict): A dictionary containing transformed datasets, where keys are table names 
            and values are DataFrames to be saved.

        Prints:
            Success messages indicating data has been saved to the database.
        """
        
        with sqlite3.connect(self.database_name) as conn:
            for table_name, df in data.items():
                if df is not None and not df.empty:
                    df.to_sql(table_name, conn, index=False, if_exists='replace')
                    print(f"Saving data to table '{table_name}' in {self.database_name}.")
            print("[SUCCESS] Data loading completed [3/3]")
            print("------------------------------------------------------------\n")

    def run_pipeline(self):
        # Extract data
        data = self.extract_data()

        # Transform data
        transformed_data = self.transform_data(data)

        # Load data: Save to SQLite
        self.save_data_to_sqlite({
            "sales_rents_2011_2021": transformed_data["sales_rents"],
            "monthly_entry_colombians_foreigners": transformed_data["tourism_1"],
            "monthly_passengers_origin": transformed_data["tourism_2"]
        })


if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.run_pipeline()
