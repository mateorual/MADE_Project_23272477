import xml.etree.ElementTree as ET
import pandas as pd
import requests
import re
from typing import Dict, List
from dataclasses import dataclass

@dataclass
class KMLFieldMapping:
    headers: List[str]
    patterns: Dict[str, str]

class KMLDataExtractor:
    """
    This class allows to take some specific KML files (reached via URLs), 
    perform transformations on them based on mappings defined for each type of dataset (input as a dictionary),
    convert them into CSV files and finally they are combined to generate a CSV file out of 22 datasets
     (11 datasets for rent offers from 2011-2021 and 11 datasets for sale offers from 2011-2021)
    """
    def __init__(self, year_mappings: Dict[int, KMLFieldMapping]):
        self.year_mappings = year_mappings

    def download_kml(self, url: str) -> ET.Element:
        try:
            response = requests.get(url)
            response.raise_for_status()
            #print(f"Successfully downloaded KML file from {url}")
            #print(f"Successfully downloaded KML file")
            return ET.fromstring(response.content)
        except requests.RequestException as e:
            print(f"Failed to download KML from {url}: {e}")
            return None

    def extract_basic_data(self, root: ET.Element) -> List[List]:
        if root is None:
            print("Error: KML root is None. Skipping extraction.")
            return []
        namespace = {'kml': 'http://www.opengis.net/kml/2.2'}
        data = []
        for placemark in root.findall(".//kml:Placemark", namespace):
            name = placemark.find("kml:name", namespace)
            description = placemark.find("kml:description", namespace)
            point = placemark.find(".//kml:Point/kml:coordinates", namespace)
            name = name.text if name is not None else "N/A"
            description = description.text if description is not None else "N/A"
            if point is not None:
                coords = point.text.strip().split(",")
                longitude, latitude = coords[0], coords[1]
                data.append([name, description, latitude, longitude])
        return data

    def parse_description(self, description: str, patterns: Dict[str, str]) -> Dict[str, str]:
        result = {}
        if pd.notna(description):
            items = [item.strip() for item in re.split(r'<br>|\n', description) if item.strip()]
            for key, pattern in patterns.items():
                for item in items:
                    match = re.search(pattern, item)
                    if match:
                        #result[key] = match.group(1).strip()
                        result[key] = match.group(match.lastindex).strip()
                        break
                if key not in result:
                    result[key] = "N/A"
        return result

    def process_year(self, year: int, url: str) -> pd.DataFrame:
        #print(f"Processing year {year} with URL: {url}")
        print(f"Processing year {year} dataset:")
        root = self.download_kml(url)
        basic_data = self.extract_basic_data(root)
        if not basic_data:
            print(f"No data extracted for year {year}")
            return pd.DataFrame()  # Return empty DataFrame if no data extracted

        df = pd.DataFrame(basic_data, columns=['Name', 'Description', 'Latitude', 'Longitude'])
        
        mapping = self.year_mappings.get(year)
        if mapping is None:
            print(f"No mapping found for year {year}. Skipping.")
            return pd.DataFrame()  # Return empty DataFrame if mapping is missing

        processed_data = []
        for _, row in df.iterrows():
            desc_info = self.parse_description(row['Description'], mapping.patterns)
            processed_row = [desc_info.get(key, "N/A") for key in mapping.headers[1:-2]] + [row['Longitude'], row['Latitude']]
            processed_data.append(processed_row)
        
        final_df = pd.DataFrame(processed_data, columns=mapping.headers[1:])
        
        # Ensure consistency across column names and keep only the desired columns
        final_df = final_df.rename(columns={
            "Tipo de Predio": "Predio",
            "Tipo Predio": "Predio",
            "Estado Predio": "Estado",
            "Tipo Investigacion": "Investigacion",
            "Tipo Invest": "Investigacion",
            "Valor M²": "Valor M2",
            "Valor MÂ²": "Valor M2"
        })
        
        # Retain only the columns of interest
        final_df = final_df.reindex(columns=[
            "Fecha", "Investigacion", "Predio", "Estado", "Barrio", "Estrato",
            "Area Privada", "Area Lote", "Valor Comercial", "Valor M2",
            "Longitude", "Latitude"
        ])
        
        #print(f"Processed {len(final_df)} rows for year {year}")
        print(f"[SUCCESS] Processed {len(final_df)} rows")
        return final_df

    def process_multiple_years(self, url_dict: Dict[int, str]) -> pd.DataFrame:
        dataframes = []
        for year, url in url_dict.items():
            if year in self.year_mappings:
                df = self.process_year(year, url)
                if not df.empty:
                    dataframes.append(df)
            else:
                print(f"Year {year} is not supported in year mappings.")
        if not dataframes:
            print("No valid dataframes to concatenate.")
            return pd.DataFrame()  # Return an empty DataFrame if none were processed
        unified_df = pd.concat(dataframes, ignore_index=True)
        return unified_df


class KMLMappings:
    # Sample Year Mappings for Sales and Rents
    # Each mapping is depend on the nature of the dataset, it varies across years
    sales_year_mappings = {
        # Populate with actual mappings as shown previously
        2011: KMLFieldMapping(
            headers=[
                "Name", "Codigo", "Fecha", "Tipo Investigacion", "Tipo Predio", 
                "Estado Predio", "Direccion", "Area Privada", "Area Lote", 
                "Valor Comercial", "Fuente", "Parqueadero", "Cuarto Util", "Latitude", "Longitude"
            ],
            patterns={
                "Codigo": r"CODIGO\s*(\d+)",
                "Fecha": r"FECHA\s*(\d{2}-\d{2}-\d{4})",
                "Tipo Investigacion": r"TIPO INVESTIGACIÓN\s*(.*)",
                "Tipo Predio": r"TIPO PREDIO\s*(.*)",
                "Estado Predio": r"ESTADO PREDIO\s*(.*)",
                "Direccion": r"DIRECCIÓN\s*(.*)",
                "Area Privada": r"AREA PRIVADA\s*(\d+)",
                "Area Lote": r"AREA LOTE\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL\s*\$(.*)",
                "Fuente": r"FUENTE\s*(.*)",
                "Parqueadero": r"PARQUEADERO\s*(.*)",
                "Cuarto Util": r"CUARTO UTIL\s*(.*)"
            }
        ),
        2012: KMLFieldMapping(
            headers=[
                "Name", "Codigo", "Fecha", "Tipo Invest", "Tipo Predio", 
                "Estado Predio", "Direccion", "Area Privada", "Area Lote", 
                "Valor Comercial", "Fuente", "Parqueadero", "Cuarto Util", "Latitude", "Longitude"
            ],
            patterns={
                "Codigo": r"CODIGO\s*(\d+)",
                "Fecha": r"FECHA\s*(\d{2}-\d{2}-\d{4})",
                "Tipo Invest": r"TIPOINVEST\s*(.*)",
                "Tipo Predio": r"TIPOPREDIO\s*(.*)",
                "Estado Predio": r"ESTADOPRED\s*(.*)",
                "Direccion": r"DIRECCIONE\s*(.*)",
                "Area Privada": r"AREAPRIVAD\s*(\d+)",
                "Area Lote": r"AREALOTE\s*(\d+)",
                "Valor Comercial": r"VALORCOMER\s*\$(.*)",
                "Fuente": r"FUENTE_1\s*(.*)",
                "Parqueadero": r"PARQUEADER\s*(.*)",
                "Cuarto Util": r"C_UTIL\s*(.*)"
            }
        ),
        2013: KMLFieldMapping(
            headers=[
                "Name", "Codigo", "Fecha", "Tipo Investigacion", "Tipo Predio", 
                "Estado Predio", "Direccion", "Area Privada", "Area Lote", 
                "Valor Comercial", "Fuente", "Parqueadero", "Cuarto Util", "Latitude", "Longitude"
            ],
            patterns={
                "Codigo": r"CODIGO\s*(\d+)",
                "Fecha": r"FECHA\s*(\d{2}-\d{2}-\d{4})",
                "Tipo Investigacion": r"TIPO INVESTIGACIÓN\s*(.*)",
                "Tipo Predio": r"TIPO PREDIO\s*(.*)",
                "Estado Predio": r"ESTADO PREDIO\s*(.*)",
                "Direccion": r"DIRECCIÓN\s*(.*)",
                "Area Privada": r"AREA PRIVADA\s*(\d+)",
                "Area Lote": r"AREA LOTE\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL\s*\$(.*)",
                "Fuente": r"FUENTE\s*(.*)",
                "Parqueadero": r"PARQUEADERO\s*(.*)",
                "Cuarto Util": r"CUARTO UTIL\s*(.*)"
            }
        ),
        2014: KMLFieldMapping(
            headers=[
                "Name", "Codigo", "Fecha", "Tipo Investigacion", "Tipo Predio", 
                "Estado Predio", "Direccion", "Area Privada", "Area Lote", 
                "Valor Comercial", "Fuente", "Parqueadero", "Cuarto Util", "Latitude", "Longitude"
            ],
            patterns={
                "Codigo": r"CODIGO\s*(\d+)",
                "Fecha": r"FECHA\s*(\d{2}-\d{2}-\d{4})",
                "Tipo Investigacion": r"TIPO INVESTIGACIÓN\s*(.*)",
                "Tipo Predio": r"TIPO PREDIO\s*(.*)",
                "Estado Predio": r"ESTADO PREDIO\s*(.*)",
                "Direccion": r"DIRECCIÓN\s*(.*)",
                "Area Privada": r"AREA PRIVADA\s*(\d+)",
                "Area Lote": r"AREA LOTE\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL\s*\$(.*)",
                "Fuente": r"FUENTE\s*(.*)",
                "Parqueadero": r"PARQUEADERO\s*(.*)",
                "Cuarto Util": r"CUARTO UTIL\s*(.*)"
            }
        ),
        2015: KMLFieldMapping(
            headers=[
                "Name", "Codigo", "Fecha", "Tipo Investigacion", "Tipo Predio", 
                "Estado Predio", "Direccion", "Area Privada", "Area Lote", 
                "Valor Comercial", "Fuente", "Parqueadero", "Cuarto Util", "Latitude", "Longitude"
            ],
            patterns={
                "Codigo": r"CODIGO\s*(\d+)",
                "Fecha": r"FECHA\s*(\d{2}-\d{2}-\d{4})",
                "Tipo Investigacion": r"TIPO INVESTIGACIÓN\s*(.*)",
                "Tipo Predio": r"TIPO PREDIO\s*(.*)",
                "Estado Predio": r"ESTADO PREDIO\s*(.*)",
                "Direccion": r"DIRECCIÓN\s*(.*)",
                "Area Privada": r"AREA PRIVADA\s*(\d+)",
                "Area Lote": r"AREA LOTE\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL\s*\$(.*)",
                "Fuente": r"FUENTE\s*(.*)",
                "Parqueadero": r"PARQUEADERO\s*(.*)",
                "Cuarto Util": r"CUARTO UTIL\s*(.*)"
            }
        ),
        2016: KMLFieldMapping(
            headers=[
                "Name", "Fecha", "Investigacion", "Predio", 
                "Estado", "Barrio", "Estrato", "Area Privada", 
                "Area Lote", "Valor Comercial", "Valor M²", "Latitude", "Longitude"
            ],
            patterns={
                "Fecha": r"FECHA:\s*(.*)",
                "Investigacion": r"INVESTIGACION:\s*(.*)",
                "Predio": r"PREDIO:\s*(.*)",
                "Estado": r"ESTADO:\s*(.*)",
                "Barrio": r"BARRIO:\s*(.*)",
                "Estrato": r"ESTRATO:\s*(\d+)",
                "Area Privada": r"AREA PRIVADA:\s*(\d+)",
                "Area Lote": r"AREA LOTE:\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL:\s*\$(.*)",
                "Valor M²": r"VALOR M²:\s*\$(.*)"
            }
        ),
        2017: KMLFieldMapping(
            headers=[
                "Name", "Fecha", "Investigacion", "Tipo Predio",
                "Estado", "Barrio", "Estrato", "Area Privada",
                "Area Lote", "Valor Comercial", "Valor M²", "Latitude", "Longitude"
            ],
            patterns={
                "Fecha": r"FECHA:\s*(.*)",
                "Investigacion": r"INVESTIGACION:\s*(.*)",
                "Tipo Predio": r"TIPO PREDIO:\s*(.*)",
                "Estado": r"ESTADO:\s*(.*)",
                "Barrio": r"BARRIO:\s*(.*)",
                "Estrato": r"ESTRATO:\s*(\d+)",
                "Area Privada": r"AREA PRIVADA:\s*(\d+)",
                "Area Lote": r"AREA LOTE:\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL:\s*\$(.*)",
                "Valor M²": r"VALOR M²:\s*\$(.*)"
            }
        ),
        2018: KMLFieldMapping(
            headers=[
                "Name", "Fecha", "Investigacion", "Predio",
                "Estado", "Barrio", "Estrato", "Area Privada",
                "Area Lote", "Valor Comercial", "Valor M²", "Latitude", "Longitude"
            ],
            patterns={
                "Fecha": r"FECHA:\s*(.*)",
                "Investigacion": r"INVESTIGACION:\s*(.*)",
                "Predio": r"PREDIO:\s*(.*)",
                "Estado": r"ESTADO:\s*(.*)",
                "Barrio": r"BARRIO:\s*(.*)",
                "Estrato": r"ESTRATO:\s*(\d+)",
                "Area Privada": r"AREA PRIVADA:\s*(\d+)",
                "Area Lote": r"AREA LOTE:\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL:\s*\$(.*)",
                "Valor M²": r"VALOR M²:\s*\$(.*)"
            }
        ),
        2019: KMLFieldMapping(
            headers=[
                "Name", "Fecha", "Investigacion", "Tipo de Predio",
                "Estado Predio", "Barrio", "Estrato", "Area Privada",
                "Area Lote", "Valor Comercial", "Valor M²", "Latitude", "Longitude"
            ],
            patterns={
                "Fecha": r"FECHA:\s*(.*)",
                "Investigacion": r"INVESTIGACION:\s*(.*)",
                "Tipo de Predio": r"TIPO DE PREDIO:\s*(.*)",
                "Estado Predio": r"ESTADO PREDIO:\s*(.*)",
                "Barrio": r"BARRIO:\s*(.*)",
                "Estrato": r"ESTRATO:\s*(\d+)",
                "Area Privada": r"AREA PRIVADA:\s*(\d+)",
                "Area Lote": r"AREA LOTE:\s*(\d+)",
                "Valor Comercial": r"VALORCO MERCIAL:\s*\$(.*)",
                #"Valor M²": r"VALOR M²:\s*\$(.*)"
                "Valor M²": r"VALOR\s?M.*?:\s*\$?\s?([0-9,]+)"
            }
        ),
        2020: KMLFieldMapping(
            headers=[
                "Name", "Fecha", "Investigacion", "Tipo de Predio", "Estado Predio",
                "Barrio", "Estrato", "Area Privada", "Area Lote",
                "Valor Comercial", "Valor M2", "Longitude", "Latitude"
            ],
            # Patterns for each field in the description, using flexible regex patterns for variations
            patterns = {
                "Fecha": r"FECHA:\s*(\d{2}-\d{2}-\d{4})",
                "Investigacion": r"INVESTIGACION:\s*(.*)",
                "Tipo de Predio": r"(?:TIPO\s*DE?\s*PREDIO|TIPO PREDIO):\s*(.*)",
                "Estado Predio": r"(?:ESTADO\s*PREDIO?|ESTADO):\s*(.*)",
                "Barrio": r"BARRIO:\s*(.*)",
                "Estrato": r"ESTRATO:\s*(\d+)",
                "Area Privada": r"AREA\s*PRIVADA:\s*(\d+)",
                "Area Lote": r"AREA\s*LOTE:\s*(\d+)",
                "Valor Comercial": r"(?:VALOR(?:CO\s*)?MERCIAL|VALOR COMERCIAL):\s*\$(.*)",
                "Valor M2": r"VALOR\s*M²:\s*\$(.*)",
                "Longitude": r"LONGITUD:\s*(-?\d+\.\d+)",
                "Latitude": r"LATITUD:\s*(-?\d+\.\d+)"
            }

        ),
        2021: KMLFieldMapping(
            headers=[
                "Name", "Fecha", "Investigacion", "Predio", "Estado",
                "Barrio", "Estrato", "Area Privada", "Area Lote",
                "Valor Comercial", "Valor M2", "Longitude", "Latitude"
            ],
            patterns = {
                "Fecha": r"FECHA:\s*(\d{2}-\d{2}-\d{4})",
                "Investigacion": r"INVESTIGACION:\s*(.*)",
                "Predio": r"PREDIO:\s*(.*)",
                "Estado": r"ESTADO:\s*(.*)",
                "Barrio": r"BARRIO:\s*(.*)",
                "Estrato": r"ESTRATO:\s*(\d+)",
                "Area Privada": r"AREA PRIVADA:\s*(\d+)",
                "Area Lote": r"AREA LOTE:\s*(\d+)",
                "Valor Comercial": r"VALORCOMERCIAL:\s*(?:\$?\s?([0-9,]+))",  # Handles "$", space, and no symbol variations
                "Valor M2": r"VALORM2:\s*\$(.*)",
                "Longitude": r"LONGITUD:\s*(-?\d+\.\d+)",
                "Latitude": r"LATITUD:\s*(-?\d+\.\d+)"
            }
            
        )
        }

    rents_year_mappings = {
        2011: KMLFieldMapping(
            headers=[
                "Name", "Codigo", "Fecha", "Tipo Investigacion", "Tipo Predio", 
                "Estado Predio", "Direccion", "Area Privada", "Area Lote", 
                "Valor Comercial", "Fuente", "Parqueadero", "Cuarto Util", "Latitude", "Longitude"
            ],
            patterns={
                "Codigo": r"CODIGO\s*(\d+)",
                "Fecha": r"FECHA\s*(\d{2}-\d{2}-\d{4})",
                "Tipo Investigacion": r"TIPO INVESTIGACIÓN\s*(.*)",
                "Tipo Predio": r"TIPO PREDIO\s*(.*)",
                "Estado Predio": r"ESTADO PREDIO\s*(.*)",
                "Direccion": r"DIRECCIÓN\s*(.*)",
                "Area Privada": r"AREA PRIVADA\s*(\d+)",
                "Area Lote": r"AREA LOTE\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL\s*\$(.*)",
                "Fuente": r"FUENTE\s*(.*)",
                "Parqueadero": r"PARQUEADERO\s*(.*)",
                "Cuarto Util": r"CUARTO UTIL\s*(.*)"
            }
        ),
        2012: KMLFieldMapping(
            headers=[
                "Name", "Codigo", "Fecha", "Tipo Investigacion", "Tipo Predio", 
                "Estado Predio", "Direccion", "Area Privada", "Area Lote", 
                "Valor Comercial", "Fuente", "Parqueadero", "Cuarto Util", "Latitude", "Longitude"
            ],
            patterns={
                "Codigo": r"CODIGO\s*(\d+)",
                "Fecha": r"FECHA\s*(\d{2}-\d{2}-\d{4})",
                "Tipo Investigacion": r"TIPO INVESTIGACIÓN\s*(.*)",
                "Tipo Predio": r"TIPO PREDIO\s*(.*)",
                "Estado Predio": r"ESTADO PREDIO\s*(.*)",
                "Direccion": r"DIRECCIÓN\s*(.*)",
                "Area Privada": r"AREA PRIVADA\s*(\d+)",
                "Area Lote": r"AREA LOTE\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL\s*\$(.*)",
                "Fuente": r"FUENTE\s*(.*)",
                "Parqueadero": r"PARQUEADERO\s*(.*)",
                "Cuarto Util": r"CUARTO UTIL\s*(.*)"
            }
        ), 
        2013: KMLFieldMapping(
            headers=[
                "Name", "Codigo", "Fecha", "Tipo Investigacion", "Tipo Predio", 
                "Estado Predio", "Direccion", "Area Privada", "Area Lote", 
                "Valor Comercial", "Fuente", "Parqueadero", "Cuarto Util", "Latitude", "Longitude"
            ],
            patterns={
                "Codigo": r"CODIGO\s*(\d+)",
                "Fecha": r"FECHA\s*(\d{2}-\d{2}-\d{4})",
                "Tipo Investigacion": r"TIPO INVESTIGACIÓN\s*(.*)",
                "Tipo Predio": r"TIPO PREDIO\s*(.*)",
                "Estado Predio": r"ESTADO PREDIO\s*(.*)",
                "Direccion": r"DIRECCIÓN\s*(.*)",
                "Area Privada": r"AREA PRIVADA\s*(\d+)",
                "Area Lote": r"AREA LOTE\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL\s*\$(.*)",
                "Fuente": r"FUENTE\s*(.*)",
                "Parqueadero": r"PARQUEADERO\s*(.*)",
                "Cuarto Util": r"CUARTO UTIL\s*(.*)"
            }
        ),
        2014: KMLFieldMapping(
            headers=[
                "Name", "Codigo", "Fecha", "Tipo Investigacion", "Tipo Predio", 
                "Estado Predio", "Direccion", "Area Privada", "Area Lote", 
                "Valor Comercial", "Fuente", "Parqueadero", "Cuarto Util", "Latitude", "Longitude"
            ],
            patterns={
                "Codigo": r"CODIGO\s*(\d+)",
                "Fecha": r"FECHA\s*(\d{2}-\d{2}-\d{4})",
                "Tipo Investigacion": r"TIPO INVESTIGACIÓN\s*(.*)",
                "Tipo Predio": r"TIPO PREDIO\s*(.*)",
                "Estado Predio": r"ESTADO PREDIO\s*(.*)",
                "Direccion": r"DIRECCIÓN\s*(.*)",
                "Area Privada": r"AREA PRIVADA\s*(\d+)",
                "Area Lote": r"AREA LOTE\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL\s*\$(.*)",
                "Fuente": r"FUENTE\s*(.*)",
                "Parqueadero": r"PARQUEADERO\s*(.*)",
                "Cuarto Util": r"CUARTO UTIL\s*(.*)"
            }
        ),
        2015: KMLFieldMapping(
            headers=[
                "Name", "Codigo", "Fecha", "Tipo Investigacion", "Tipo Predio", 
                "Estado Predio", "Direccion", "Area Privada", "Area Lote", 
                "Valor Comercial", "Fuente", "Parqueadero", "Cuarto Util", "Latitude", "Longitude"
            ],
            patterns={
                "Codigo": r"CODIGO\s*(\d+)",
                "Fecha": r"FECHA\s*(\d{2}-\d{2}-\d{4})",
                "Tipo Investigacion": r"TIPO INVESTIGACIÓN\s*(.*)",
                "Tipo Predio": r"TIPO PREDIO\s*(.*)",
                "Estado Predio": r"ESTADO PREDIO\s*(.*)",
                "Direccion": r"DIRECCIÓN\s*(.*)",
                "Area Privada": r"AREA PRIVADA\s*(\d+)",
                "Area Lote": r"AREA LOTE\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL\s*\$(.*)",
                "Fuente": r"FUENTE\s*(.*)",
                "Parqueadero": r"PARQUEADERO\s*(.*)",
                "Cuarto Util": r"CUARTO UTIL\s*(.*)"
            }
        ),
        2016: KMLFieldMapping(
            headers=[
                "Name", "Fecha", "Tipo Investigacion", "Tipo Predio", 
                "Estado Predio", "Barrio", "Estrato", "Area Privada", 
                "Area Lote", "Valor Comercial", "Valor M2", "Latitude", "Longitude"
            ],
            patterns = {
                "Fecha": r"FECHA:\s*(\d{2}-\d{2}-\d{4})",
                "Tipo Investigacion": r"TIPOINVESTIGACION:\s*(.*)",
                "Tipo Predio": r"TIPOPREDIO:\s*(.*)",
                "Estado Predio": r"ESTADOPREDIO:\s*(.*)",
                "Barrio": r"BARRIO:\s*(.*)",
                "Estrato": r"ESTRATO:\s*(\d+)",
                "Area Privada": r"AREA PRIVADA:\s*(\d+)",
                "Area Lote": r"AREA LOTE:\s*(\d+)",
                "Valor Comercial": r"VALORCOMERCIAL:\s*\$(.*)",
                "Valor M2": r"VALOR\s?M[^A-Za-z0-9]?[²]?:\s*\$?\s?([0-9,]+)",  # Highly flexible pattern for "Valor M2"
                "Latitude": r"LATITUD:\s*(\S+)",
                "Longitude": r"LONGITUD:\s*(\S+)"
            }
        ),
        2017: KMLFieldMapping(
            headers=[
                "Name", "Fecha", "Investigacion", "Tipo Predio",
                "Estado", "Barrio", "Estrato", "Area Privada",
                "Area Lote", "Valor Comercial", "Valor M²", "Latitude", "Longitude"
            ],
            patterns={
                "Fecha": r"FECHA:\s*(.*)",
                "Investigacion": r"INVESTIGACION:\s*(.*)",
                "Tipo Predio": r"TIPO PREDIO:\s*(.*)",
                "Estado": r"ESTADO:\s*(.*)",
                "Barrio": r"BARRIO:\s*(.*)",
                "Estrato": r"ESTRATO:\s*(\d+)",
                "Area Privada": r"AREA PRIVADA:\s*(\d+)",
                "Area Lote": r"AREA LOTE:\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL:\s*\$(.*)",
                "Valor M²": r"VALOR M²:\s*\$(.*)"
            }
        ),
        2018: KMLFieldMapping(
            headers=[
                "Name", "Fecha", "Investigacion", "Predio",
                "Estado", "Barrio", "Estrato", "Area Privada",
                "Area Lote", "Valor Comercial", "Valor M²", "Latitude", "Longitude"
            ],
            patterns={
                "Fecha": r"FECHA:\s*(.*)",
                "Investigacion": r"INVESTIGACION:\s*(.*)",
                "Predio": r"PREDIO:\s*(.*)",
                "Estado": r"ESTADO:\s*(.*)",
                "Barrio": r"BARRIO:\s*(.*)",
                "Estrato": r"ESTRATO:\s*(\d+)",
                "Area Privada": r"AREA PRIVADA:\s*(\d+)",
                "Area Lote": r"AREA LOTE:\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL:\s*\$(.*)",
                "Valor M²": r"VALOR M²:\s*\$(.*)"
            }
        ),
        2019: KMLFieldMapping(
            headers=[
                "Name", "Fecha", "Investigacion", "Tipo de Predio",
                "Estado Predio", "Barrio", "Estrato", "Area Privada",
                "Area Lote", "Valor Comercial", "Valor M²", "Latitude", "Longitude"
            ],
            patterns={
                "Fecha": r"FECHA:\s*(.*)",
                "Investigacion": r"INVESTIGACION:\s*(.*)",
                "Tipo de Predio": r"TIPO DE PREDIO:\s*(.*)",
                "Estado Predio": r"ESTADO PREDIO:\s*(.*)",
                "Barrio": r"BARRIO:\s*(.*)",
                "Estrato": r"ESTRATO:\s*(\d+)",
                "Area Privada": r"AREA PRIVADA:\s*(\d+)",
                "Area Lote": r"AREA LOTE:\s*(\d+)",
                "Valor Comercial": r"VALORCO MERCIAL:\s*\$(.*)",
                "Valor M²": r"VALOR M²:\s*\$(.*)"
            }
        ),
        2020: KMLFieldMapping(
            headers=[
                "Name", "Fecha", "Investigacion", "Tipo Predio", 
                "Estado", "Barrio", "Estrato", "Area Privada", 
                "Area Lote", "Valor Comercial", "Valor M2", "Latitude", "Longitude"
            ],
            patterns={
                "Fecha": r"FECHA:\s*(\d{2}-\d{2}-\d{4})",
                "Investigacion": r"INVESTIGACION:\s*(.*)",
                "Tipo Predio": r"TIPO PREDIO:\s*(.*)",
                "Estado": r"ESTADO:\s*(.*)",
                "Barrio": r"BARRIO:\s*(.*)",
                "Estrato": r"ESTRATO:\s*(\d+)",
                "Area Privada": r"AREA PRIVADA:\s*(\d+)",
                "Area Lote": r"AREA LOTE:\s*(\d+)",
                "Valor Comercial": r"VALOR COMERCIAL:\s*\$(.*)",
                #"Valor M2": r"VALOR MÂ²:\s*\$(.*)",
                "Valor M2": r"VALOR\s?M[^A-Za-z0-9]?[²]?:\s*\$?\s?([0-9,]+)",  # Highly flexible pattern for "Valor M2"
                "Latitude": r"LATITUD:\s*(\S+)",
                "Longitude": r"LONGITUD:\s*(\S+)"
            }
        ),
        2021: KMLFieldMapping(
            headers=[
                "Name", "Fecha", "Investigacion", "Predio", "Estado",
                "Barrio", "Estrato", "Area Privada", "Area Lote",
                "Valor Comercial", "Valor M2", "Longitude", "Latitude"
            ],
            patterns = {
                "Fecha": r"FECHA:\s*(\d{2}-\d{2}-\d{4})",
                "Investigacion": r"INVESTIGACION:\s*(.*)",
                "Predio": r"PREDIO:\s*(.*)",
                "Estado": r"ESTADO:\s*(.*)",
                "Barrio": r"BARRIO:\s*(.*)",
                "Estrato": r"ESTRATO:\s*(\d+)",
                "Area Privada": r"AREA PRIVADA:\s*(\d+)",
                "Area Lote": r"AREA LOTE:\s*(\d+)",
                "Valor Comercial": r"VALORCOMERCIAL:\s*(?:\$?\s?([0-9,]+))",  # Handles "$", space, and no symbol variations
                "Valor M2": r"VALORM2:\s*\$(.*)",
                "Longitude": r"LONGITUD:\s*(-?\d+\.\d+)",
                "Latitude": r"LATITUD:\s*(-?\d+\.\d+)"
            }
        )
    }

    

