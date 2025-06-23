import argparse
import logging
import csv
from io import StringIO

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
from datetime import datetime

# --- Funciones de Transformación y Clases ---

class ParseCsvDoFn(beam.DoFn):
    """
    Parsea cada línea de un archivo CSV a un diccionario.
    Asume que la primera línea es el encabezado y la ignora.
    """
    def process(self, element):
        # element es una tupla (offset, line) si se lee con skip_header_lines=1
        line = element[1]
        try:
            # Usa StringIO para tratar la línea como un archivo en memoria
            reader = csv.reader(StringIO(line))
            parsed_line = next(reader)
            
            # Sanitiza los nombres de las columnas
            headers = ['store_id', 'product_id', 'quantity', 'unit_price', 'sale_date', 'currency']
            
            # Crea el diccionario
            row = {header.strip(): value.strip() for header, value in zip(headers, parsed_line)}
            yield row
        except Exception as e:
            logging.error(f"No se pudo parsear la línea: {line}. Error: {e}")


class EnrichWithExchangeRate(beam.DoFn):
    """
    Enriquece los datos de ventas con la tasa de cambio correcta.
    Utiliza la tabla de tasas de cambio como un Side Input.
    """
    def process(self, element, exchange_rates_list):
        try:
            sale_date_str = element['sale_date']
            sale_currency = element['currency']
            sale_date = datetime.strptime(sale_date_str, '%Y-%m-%d').date()

            rate_to_usd = None
            for rate_info in exchange_rates_list:
                # Compara que la moneda sea la misma y la fecha de venta esté en el rango válido
                if (rate_info['currency'] == sale_currency and
                    rate_info['valid_from'] <= sale_date <= rate_info['valid_to']):
                    rate_to_usd = float(rate_info['rate_to_usd'])
                    break
            
            if rate_to_usd is None:
                raise ValueError(f"No se encontró tasa de cambio para {sale_currency} en la fecha {sale_date_str}")

            # Calcula el monto en USD
            quantity = int(element['quantity'])
            unit_price = float(element['unit_price'])
            amount_usd = quantity * unit_price * rate_to_usd

            # Prepara el elemento de salida con el esquema correcto para BigQuery
            yield {
                'store_id': int(element['store_id']),
                'product_id': element['product_id'],
                'quantity': quantity,
                'unit_price': unit_price,
                'sale_date': sale_date_str,
                'currency': sale_currency,
                'rate_to_usd': rate_to_usd,
                'amount_usd': round(amount_usd, 2),
                'processing_timestamp': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            }

        except Exception as e:
            logging.error(f"Error enriqueciendo el elemento {element}. Error: {e}")


def run(argv=None):
    """Función principal para construir y ejecutar el pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_file',
        dest='input_file',
        required=True,
        help='retail-data-zone/sales/2025-06-22.csv')
    parser.add_argument(
        '--output_table',
        dest='output_table',
        required=True,
        help='directed-gasket-309507.retail.analytics_sales_usd')
    parser.add_argument(
        '--exchange_rates_table',
        dest='exchange_rates_table',
        required=True,
        help='directed-gasket-309507.retail.ref_exchange_rates')

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    # Definición del esquema de la tabla destino para BigQuery
    table_schema = bigquery.TableSchema()
    fields = [
        bigquery.TableFieldSchema(name='store_id', type='INTEGER', mode='REQUIRED'),
        bigquery.TableFieldSchema(name='product_id', type='STRING', mode='REQUIRED'),
        bigquery.TableFieldSchema(name='quantity', type='INTEGER', mode='REQUIRED'),
        bigquery.TableFieldSchema(name='unit_price', type='FLOAT', mode='REQUIRED'),
        bigquery.TableFieldSchema(name='sale_date', type='DATE', mode='REQUIRED'),
        bigquery.TableFieldSchema(name='currency', type='STRING', mode='REQUIRED'),
        bigquery.TableFieldSchema(name='rate_to_usd', type='FLOAT', mode='REQUIRED'),
        bigquery.TableFieldSchema(name='amount_usd', type='FLOAT', mode='REQUIRED'),
        bigquery.TableFieldSchema(name='processing_timestamp', type='TIMESTAMP', mode='REQUIRED'),
    ]
    table_schema.fields.extend(fields)


    with beam.Pipeline(options=pipeline_options) as p:
        # 1. Cargar la tabla de referencia de tasas de cambio como un Side Input
        exchange_rates = (
            p | 'ReadExchangeRates' >> beam.io.ReadFromBigQuery(
                    table=known_args.exchange_rates_table,
                    method=beam.io.ReadFromBigQuery.Method.DIRECT_READ
                )
        )

        # 2. Leer archivos de ventas desde GCS y procesarlos
        sales_data = (
            p | 'ReadSalesCsv' >> beam.io.textio.ReadFromTextWithFilename(known_args.input_file, skip_header_lines=1)
              | 'ParseCsv' >> beam.ParDo(ParseCsvDoFn())
        )

        # 3. Enriquecer los datos de ventas y calcular el monto en USD
        enriched_sales = (
            sales_data | 'EnrichAndTransform' >> beam.ParDo(
                EnrichWithExchangeRate(), 
                exchange_rates_list=beam.pvalue.AsList(exchange_rates) # El side input se pasa aquí
            )
        )

        # 4. Cargar los datos transformados a BigQuery
        enriched_sales | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table=known_args.output_table,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()