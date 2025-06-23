-- DDL para crear la tabla de referencia de tasas de cambio
CREATE OR REPLACE TABLE retail.ref_exchange_rates
(
  currency STRING NOT NULL,
  rate_to_usd FLOAT64 NOT NULL,
  valid_from DATE NOT NULL,
  valid_to DATE NOT NULL
);

-- Insertar datos de ejemplo para que el pipeline funcione
INSERT INTO retail.ref_exchange_rates (currency, rate_to_usd, valid_from, valid_to)
VALUES
  ('EUR', 1.07, '2025-05-01', '2025-05-10'),
  ('EUR', 1.08, '2025-05-11', '2025-05-20'),
  ('ARS', 0.0011, '2025-05-01', '2025-05-31'),
  ('USD', 1.0, '2020-01-01', '2099-12-31'); -- Tasa para USD por si viene en los datos
