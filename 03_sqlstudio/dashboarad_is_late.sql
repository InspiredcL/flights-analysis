-- Crea dimensión IS_LATE de la vista flights para el informe (dashboard o reporte)

CASE
    WHEN ARR_DELAY < 15 THEN "ON TIME"
    ELSE "LATE"
END

-- esto se aplica a todas las fuentes de datos:
    -- flights
    -- delayed_10
    -- delayed_15
    -- delayed_20

