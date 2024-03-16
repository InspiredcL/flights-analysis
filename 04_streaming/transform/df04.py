#!/usr/bin/env python3

# Copyright 2016 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import csv
import json
from pytz.exceptions import UnknownTimeZoneError
import apache_beam as beam


def addtimezone(lat, lon):
    """
    Agrega la zona horaria correspondiente a las coordenadas proporcionadas.

    **Argumentos:**
    * `lat`: Latitud en grados decimales.
    * `lon`: Longitud en grados decimales.

    **Devuelve:**
    Una tupla con las coordenadas y la zona horaria correspondiente.

    **Excepción:**
    * `ValueError`: Si las coordenadas no son válidas.

    **Ejemplo:**
        addtimezone(-33.45, -70.66)
        (-33.45, -70.66, 'America/Santiago')

    **Documentación adicional:**
    * La función utiliza la librería `timezonefinder` para obtener la zona
    horaria correspondiente a las coordenadas proporcionadas.
    * La función maneja la excepción `ValueError` en caso de que las coordenadas no sean válidas.
    """

    try:
        import timezonefinder  # pylint: disable=import-outside-toplevel
        tf = timezonefinder.TimezoneFinder()
        lat = float(lat)
        lon = float(lon)
        return lat, lon, tf.timezone_at(lng=lon, lat=lat)
    except (ValueError, UnknownTimeZoneError):
        return lat, lon, 'TIMEZONE'  # header


def as_utc(date, hhmm, tzone):
    """
    Convierte una fecha y hora en formato UTC a la hora corregida para una zona horaria específica.

    **Argumentos:**

    * `date`: Fecha en formato `YYYY-MM-DD`.
    * `hhmm`: Hora en formato `HH:MM`.
    * `tzone`: Zona horaria en formato `TZ`.

    **Devuelve:**

    * `utc_dt`: Objeto de fecha y hora en formato `YYYY-MM-DDTHH:MM:SS+00:00`.
    * `tz_offset`: Desplazamiento de la zona horaria en segundos.

    **Excepción:**

    * `ValueError`: Si la fecha, la hora o la zona horaria no son válidas.
    """

    try:
        if len(hhmm) > 0 and tzone is not None:
            import datetime  # pylint: disable=import-outside-toplevel
            import pytz  # pylint: disable=import-outside-toplevel
            loc_tz = pytz.timezone(tzone)
            loc_dt = loc_tz.localize(
                datetime.datetime.strptime(date, '%Y-%m-%d'),
                is_dst=False
            )
            # can't just parse hhmm because the data contains 2400 and the like ...
            loc_dt += datetime.timedelta(
                hours=int(hhmm[:2]), minutes=int(hhmm[2:]))
            utc_dt = loc_dt.astimezone(pytz.utc)
            return (
                utc_dt.strftime('%Y-%m-%d %H:%M:%S'),
                loc_dt.utcoffset().total_seconds()
            )
        else:
            # Vuelos cancelados y offset de 0
            print("Devolviendo ('', 0) porque hhmm está vacío or tzone es None")
            return '', 0
    except ValueError as e:
        logging.exception("%s %s %s ValueError: %s", date, hhmm, tzone, e)
        print("Exception occurred in as_utc:", e)
        return None



def add_24h_if_before(arrtime, deptime):
    """
    add_24h_if_before 

    _extended_summary_

    Arguments:
        arrtime -- _description_
        deptime -- _description_

    Returns:
        _description_
    """

    import datetime  # pylint: disable=import-outside-toplevel
    if len(arrtime) > 0 and len(deptime) > 0 and arrtime < deptime:
        adt = datetime.datetime.strptime(arrtime, '%Y-%m-%d %H:%M:%S')
        adt += datetime.timedelta(hours=24)
        return adt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        return arrtime


def tz_correct(line, airport_timezones):
    """
    Realiza un ajuste de zonas horarias para los campos de fecha y hora de un
    diccionario de datos de vuelo.

    **Argumentos:**

    * `fields`: Diccionario que contiene los datos de vuelo, incluyendo
    campos de fecha y hora.
    * `airport_timezones`: Diccionario que mapea los identificadores de
    aeropuerto a sus respectivas zonas horarias.

    **Devuelve:**

    * Un generador que produce un diccionario con los campos de fecha y hora ajustados a UTC.

    **Proceso:**

    1. Convierte la fecha de vuelo a una cadena en formato `YYYY-MM-DD`.
    2. Obtiene las zonas horarias de los aeropuertos de salida y llegada.
    3. Convierte los tiempos de salida a UTC.
    4. Convierte los tiempos de llegada a UTC.
    5. Corrige los tiempos de llegada que sean anteriores a los tiempos
    de salida, agregando 24 horas.
    6. Agrega los desplazamientos de zona horaria de los aeropuertos de
    salida y llegada al diccionario de datos.
    """

    fields = json.loads(line)
    try:
        # convert all times to UTC
        dep_airport_id = fields["ORIGIN_AIRPORT_SEQ_ID"]
        arr_airport_id = fields["DEST_AIRPORT_SEQ_ID"]
        dep_timezone = airport_timezones[dep_airport_id][2]
        arr_timezone = airport_timezones[arr_airport_id][2]

        for f in ["CRS_DEP_TIME", "DEP_TIME", "WHEELS_OFF"]:
            fields[f], deptz = as_utc(
                fields["FL_DATE"],
                fields[f],
                dep_timezone
            )  # type: ignore

        for f in ["WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f], arrtz = as_utc(
                fields["FL_DATE"],
                fields[f],
                arr_timezone
            )  # type: ignore

        for f in ["WHEELS_OFF", "WHEELS_ON", "CRS_ARR_TIME", "ARR_TIME"]:
            fields[f] = add_24h_if_before(
                fields[f],
                fields["DEP_TIME"]
            )

        fields["DEP_AIRPORT_LAT"] = airport_timezones[dep_airport_id][0]
        fields["DEP_AIRPORT_LON"] = airport_timezones[dep_airport_id][1]
        fields["DEP_AIRPORT_TZOFFSET"] = deptz
        fields["ARR_AIRPORT_LAT"] = airport_timezones[arr_airport_id][0]
        fields["ARR_AIRPORT_LON"] = airport_timezones[arr_airport_id][1]
        fields["ARR_AIRPORT_TZOFFSET"] = arrtz
        yield json.dumps(fields)
    except KeyError as e:
        # En caso de que falte una clave en el diccionario, registramos una excepción.
        logging.exception(
            " Ignorando %s aeropuerto no conocido, KeyError Error: %s",
            line,
            e
        )


if __name__ == '__main__':
    with beam.Pipeline('DirectRunner') as pipeline:
        airports = (pipeline
                    | 'airports:read' >> beam.io.ReadFromText(
                        'airports_2024.csv.gz'
                    )
                    | beam.Filter(lambda line: "United States" in line)
                    | 'airports:fields' >> beam.Map(
                        lambda line: next(
                            csv.reader([line])
                        )
                    )
                    | 'airports:tz' >> beam.Map(
                        lambda fields: (
                            fields[0],
                            addtimezone(fields[21], fields[26])
                        )
                    )
                    )
        flights = (pipeline
                   | 'flights:read' >> beam.io.ReadFromText(
                       'flights_sample_2024.json'
                   )
                   | 'flights:tzcorr' >> beam.FlatMap(
                       tz_correct,
                       beam.pvalue.AsDict(
                           airports)  #type: ignore
                   )
                   )

        all_flights = (flights
                       | beam.io.textio.WriteToText('df04_all_flights')  # pylint: disable=reportAttributeAccessIssue
                       )
