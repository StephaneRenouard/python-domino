import logging
import time
from datetime import datetime

import requests
from influxdb import InfluxDBClient


print("hello")




# clés téléinfo
INT_MESURE_KEYS = ['BASE', 'IMAX', 'HCHC', 'IINST', 'PAPP', 'ISOUSC', 'ADCO', 'HCHP']

# création du logguer
logging.basicConfig(filename="C:/var/log/teleinfo/releve.log", level=logging.INFO, format='%(asctime)s %(message)s')
logging.info("Teleinfo starting..")


# connexion a la base de données InfluxDB
client = InfluxDBClient('localhost', 8086)
DB_NAME = "teleinfo"
connected = False
while not connected:
    try:
        logging.info("Database %s exists?" % DB_NAME)
        if not {'name': DB_NAME} in client.get_list_database():
            logging.info("Database %s creation.." % DB_NAME)
            client.create_database(DB_NAME)
            logging.info("Database %s created!" % DB_NAME)
        client.switch_database(DB_NAME)
        logging.info("Connected to %s!" % DB_NAME)
    except requests.exceptions.ConnectionError:
        logging.info('InfluxDB is not reachable. Waiting 5 seconds to retry.')
        time.sleep(5)
    else:
        connected = True


def add_measures(measures, time_measure):
    points = []
    for measure, value in measures.items():
        point = {
            "measurement": measure,
            "tags": {
                # identification de la sonde et du compteur
                "host": "raspberry",
                "region": "linky"
            },
            "time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "fields": {
                "value": value
            }
        }
        points.append(point)

    client.write_points(points)


def verif_checksum(data, checksum):
    data_unicode = 0
    for caractere in data:
        data_unicode += ord(caractere)
    sum_unicode = (data_unicode & 63) + 32
    return (checksum == chr(sum_unicode))


def main():
    with serial.Serial(port='/dev/ttyUSB0', baudrate=1200, parity=serial.PARITY_NONE, stopbits=serial.STOPBITS_ONE,
                       bytesize=serial.SEVENBITS, timeout=1) as ser:

        logging.info("Teleinfo is reading on /dev/ttyUSB0..")

        trame = dict()

        # boucle pour partir sur un début de trame
        line = ser.readline()
        while b'\x02' not in line:  # recherche du caractère de début de trame
            line = ser.readline()

        # lecture de la première ligne de la première trame
        line = ser.readline()

        while True:
            line_str = line.decode("utf-8")
            logging.debug(line)

            try:
                # separation sur espace /!\ attention le caractere de controle 0x32 est un espace aussi
                [key, val, *_] = line_str.split(" ")

                # supprimer les retours charriot et saut de ligne puis selectionne le caractere
                # de controle en partant de la fin
                checksum = (line_str.replace('\x03\x02', ''))[-3:-2]

                if verif_checksum(f"{key} {val}", checksum):
                    # creation du champ pour la trame en cours avec cast des valeurs de mesure en "integer"
                    trame[key] = int(val) if key in INT_MESURE_KEYS else val

                if b'\x03' in line:  # si caractère de fin dans la ligne, on insère la trame dans influx
                    del trame['ADCO']  # adresse du compteur : confidentiel!
                    time_measure = time.time()

                    # insertion dans influxdb
                    add_measures(trame, time_measure)

                    # ajout timestamp pour debugger
                    trame["timestamp"] = int(time_measure)
                    logging.debug(trame)

                    trame = dict()  # on repart sur une nouvelle trame
            except Exception as e:
                logging.error("Exception : %s" % e, exc_info=True)
                logging.error("%s %s" % (key, val))
            line = ser.readline()


if __name__ == '__main__':
    if connected:
        main()
