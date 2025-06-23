-- DDL para crear la tabla destino de ventas en USD
-- La tabla está particionada por día en la columna 'sale_date' para optimizar costos y rendimiento.
-- El clustering por store_id y product_id acelera las consultas que filtran o agrupan por estas columnas.
CREATE OR REPLACE TABLE retail.analytics_sales_usd
(
  store_id INT64 NOT NULL,
  product_id STRING NOT NULL,
  quantity INT64 NOT NULL,
  unit_price FLOAT64 NOT NULL,
  sale_date DATE NOT NULL,
  currency STRING NOT NULL,
  rate_to_usd FLOAT64 NOT NULL,
  amount_usd FLOAT64 NOT NULL,
  processing_timestamp TIMESTAMP NOT NULL
)
PARTITION BY sale_date
CLUSTER BY store_id, product_id
OPTIONS(
  partition_expiration_days=1825, -- 5 años, ajustar según la política de retención
  description="Tabla analítica de ventas diarias convertidas a USD."
);
