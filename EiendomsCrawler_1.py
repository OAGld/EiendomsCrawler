import requests
from bs4 import BeautifulSoup
import mysql.connector
import time
import threading
from concurrent.futures import ThreadPoolExecutor
import logging
from tqdm import tqdm
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import random
import re
import Auxiliary
import json

# TODO
# - Check to see if its any point in using mysql.connector.connect in every thread that tries to access the database
# - Progression bar is not displayed in the nohup file anymore, add progression bar somewhere

# GLOBALS
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept-Language": "nb-NO,nb;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
})

_retry = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=1.0,
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"],
)

SESSION.mount("https://", HTTPAdapter(max_retries=_retry, pool_connections=80, pool_maxsize=80))

CONFIG_LOCK = threading.Lock()


def main():
    start_time = time.time()

    #get parameters from INI file
    useFile, ad_list, start, finish, configData = Auxiliary.initialise()

    #run program until internet connection is lost or the program finishes
    connectionTries = 0
    finished = False
    while(not finished):

        connectionTries = 0

        while connected() and not finished:
            
            #Loop through all defined ads and adds to database
            if(useFile == "True"):
                index = 0
                for i in tqdm (ad_list, desc="Progress"):
                    i = i.rstrip('\r')

                    #Append progression to config file
                    Auxiliary.appendProgress(configData, useFile, i, None)

                    BoligInstance = extract(i, configData, str(index))
                    index += 1
                    if (index == len(ad_list)):
                        finished = True

            else:
                def worker(i):
                    print(i)
                    try:
                        URL = "".join(("https://www.finn.no/realestate/homes/ad.html?finnkode=", str(i)))

                        #Append progression to config file
                        with CONFIG_LOCK:
                            Auxiliary.appendProgress(configData, useFile, URL, i)

                        # Extract and store data
                        extract(URL, configData, str(i))

                    except Exception as e:
                        logging.error(f"{i} - Error in worker: {e}")

                with ThreadPoolExecutor(max_workers=80) as executor:
                    count = 0
                    for _ in tqdm(executor.map(worker, range(start, finish + 1)), total=finish - start + 1, desc="Progress"):
                        count += 1
                        if count >= finish - start + 1:
                            finished = True

        if(not finished):
            time.sleep(6)
            connectionTries += 1
            if (connectionTries == 10):
                with CONFIG_LOCK:
                    logging.error("".join((configData.get("progress", "progression"), " - ", "Unable to connect to 'www.finn.no', exiting program.")))
                finished = True

    logging.info("Process finished --- %s seconds ---" % (time.time() - start_time))
    print("Process finished --- %s seconds ---" % (time.time() - start_time))

def connected():
    try:
        response = SESSION.get("https://www.finn.no", timeout=5)
        logging.info("connected to 'www.Finn.no'")
        return True
    except (requests.ConnectionError, requests.exceptions.SSLError):
        logging.info("Unable to connect to 'www.Finn.no'")
        return False 
 
class store:

    def __init__(self, Bolig, progress):
        #Bolig is the inherited instance
        self.Bolig = Bolig
        self.progress = progress
        store.writeToDatabase(self, Bolig)

    def writeToDatabase(self, Bolig):

        COLUMNS = [
            ("Finnkode", "BIGINT", lambda b: int(b.URL.split("finnkode=")[1].split("&")[0])),
            ("Link", "VARCHAR(255)", lambda b: b.URL),
            ("Adresse", "VARCHAR(255)", lambda b: getattr(b, "adresse", None)),

            ("Prisantydning", "VARCHAR(255)", lambda b: getattr(b, "prisantydning", None)),
            ("Totalpris", "VARCHAR(255)", lambda b: getattr(b, "totalpris", None)),
            ("Felleskostnader", "VARCHAR(255)", lambda b: getattr(b, "felleskostnader", None)),
            ("KommunaleAvgifter", "VARCHAR(255)", lambda b: getattr(b, "kommunaleAvgifter", None)),

            ("Boligtype", "VARCHAR(255)", lambda b: getattr(b, "boligtype", None)),
            ("Eierform", "VARCHAR(255)", lambda b: getattr(b, "eierform", None)),

            ("AntallSoverom", "INT", lambda b: getattr(b, "soverom", None)),
            ("ArealPrimerrom", "INT", lambda b: getattr(b, "primerrom", None)),
            ("Bruksareal", "INT", lambda b: getattr(b, "bruksareal", None)),

            ("Etasje", "VARCHAR(255)", lambda b: getattr(b, "etasje", None)),
            ("Byggear", "VARCHAR(255)", lambda b: getattr(b, "byggear", None)),
            ("Energimerke", "VARCHAR(255)", lambda b: getattr(b, "energimerke", None)),

            ("AntallRom", "INT", lambda b: getattr(b, "rom", None)),
            ("Parkering", "VARCHAR(5)", lambda b: getattr(b, "parkering", None)),
            ("Balkong", "VARCHAR(5)", lambda b: getattr(b, "balkong", None)),

            ("Tomteareal", "VARCHAR(255)", lambda b: getattr(b, "plotArea", None)),

            ("Beskrivelse", "TEXT", lambda b: getattr(b, "beskrivelse", None)),

            ("Tidligerekjøp", "TEXT", lambda b: json.dumps(getattr(b, "Tidligerekjop", None))),
            ("SistEndret", "VARCHAR(255)", lambda b: getattr(b, "sistEndret", None)),

            ("Standard_Bad", "DECIMAL(2,1)", lambda b: getattr(b, "standard_bad", None)),
            ("Standard_Kjøkken", "DECIMAL(2,1)", lambda b: getattr(b, "standard_kjokken", None)),
            ("Standard_Bolig", "DECIMAL(2,1)", lambda b: getattr(b, "standard_bolig", None)),

            ("SistEndretDT", "DATETIME", lambda b: getattr(b, "sistEndretDT", None)),
        ]

        table = "data"
        col_names = [n for n, _, _ in COLUMNS]
        col_defs  = ", ".join(f"`{n}` {t}" for n, t, _ in COLUMNS)

        # Creates the table if it does not already exist, including an auto-increment primary key and a composite unique constraint
        # on (Finnkode, SistEndret) to prevent duplicate entries for the same listing snapshot.
        create_sql = (
            f"CREATE TABLE IF NOT EXISTS `{table}` ("
            f"`id` BIGINT NOT NULL AUTO_INCREMENT, "
            f"{col_defs}, "
            f"PRIMARY KEY (`id`), "
            f"UNIQUE KEY `uniq_finnkode_sistendret` (`Finnkode`, `SistEndret`)"
            f")"
        )
        insert_sql = (
            f"INSERT IGNORE INTO `{table}` "
            f"({', '.join(f'`{c}`' for c in col_names)}) "
            f"VALUES ({', '.join(['%s']*len(col_names))})"
        )
        values = tuple(get(Bolig) for _, _, get in COLUMNS)

        mydb = mycursor = None
        try:
            with CONFIG_LOCK:
                mydb = mysql.connector.connect(
                    host=Bolig.configData.get("mysql", "host"),
                    user=Bolig.configData.get("mysql", "user"),
                    password=Bolig.configData.get("mysql", "password"),
                    database=Bolig.configData.get("mysql", "database"),
                )
            mycursor = mydb.cursor()
            mycursor.execute(create_sql)
            mycursor.execute(insert_sql, values)
            mydb.commit()
            logging.info(f"{self.progress} - {int(Bolig.URL.split('finnkode=')[1].split('&')[0])} successfully added to table.")
        except Exception as e:
            logging.error(f"{self.progress} - Error when writing to database: {e}")
        finally:
            if mycursor: 
                mycursor.close()
            if mydb: 
                mydb.close()


class extract:

    #all values
    def __init__(self, URL, configData, progress):
        self.URL = URL
        self.adresse = None
        self.postnummer = None
        self.poststed = None
        self.leiepris = None
        self.depositum = None
        self.boligtype = None
        self.soverom = None
        self.primerrom = None
        self.bruksareal = None
        self.etasje = None
        self.energimerke = None
        self.rom = None
        self.parkering = None
        self.balkong = None
        self.eierhistorie_URL = None #URL som brukes til � finne forrige kj�pssum og dato
        self.plotArea = None
        self.beskrivelse = None
        self.Tidligerekjop = []
        self.configData = configData
        self.progress = progress
        self.sistEndret = None
        self.utleid = None
        self.standard = None
        self.sistEndretDT = None

        web_page = SESSION.get(URL, timeout=10)

        if(extract.adpage_exists(self, web_page)): #returns true if the ad exists
            extract.extract_adpage(self, web_page)
            extract.extract_ownership_history(self, self.eierhistorie_URL)

            #Store data if ad exists, passes the current instance to store class
            store(self, progress)

            return
        return

    def adpage_exists(self, web_page):
        
        try:
            adpage = BeautifulSoup(web_page.content, "html.parser")

            # Positive check: a real ad page contains the detailed section or FINN-kode text.
            exists = (
                adpage.find("section", attrs={"aria-label": "Detaljert informasjon om bolig"}) is not None
                or adpage.find(string=re.compile(r"\bFINN-kode\b", re.I)) is not None
            )
            
            return exists
        except Exception as e:
            logging.error("".join((self.progress, " - ", "Error checking ad page existence. Exception thrown: ", str(e))))

    def extract_adpage(self, web_page):

        #Parse page
        try:
            adpage = BeautifulSoup(web_page.content, 'html.parser')
        except Exception as e:
            logging.error("".join((self.progress, " - ", "Error with GET request or parsing page. Exception thrown: ", str(e))))


        #Find tags containing all relevant data
        try:
            tag1 = adpage.find('body')
            tag2 = tag1.find('main')
            tag3 = tag2.find('section', attrs={'aria-label': 'Detaljert informasjon om bolig'})
        except Exception as e:
            logging.error("".join((self.progress, " - ", "Error getting main tags for extraction. Exception thrown: ", str(e))))

        #Find sist endret and create a datetime version of it as well
        try:
            SistEndretMain = tag2.find('section', attrs={'aria-labelledby': 'ad-info-heading'})
            SistEndretList = SistEndretMain.find_all('tr')
            SistEndret = SistEndretList[1].find('td')
            self.sistEndret = SistEndret.text.strip()
            self.sistEndretDT = Auxiliary.parse_norwegian_date(self.sistEndret)
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error extracting 'sist endret dato'. Exception thrown: ", str(e))))

        #Extract address
        try:
            address_section = tag3.find('section', attrs={'aria-label': 'Tittel'})
            self.adresse = address_section.find('span', attrs={'data-testid': 'object-address'}).text.strip()
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error extracting address. Exception thrown: ", str(e))))

        #Extract price info
        try:
            prisdetaljer_section = tag3.find('section', attrs={'aria-label': 'Prisdetaljer'})
            prisdetaljer_list = prisdetaljer_section.find('dl')

            # Finn prisantydning
            prisantydning_section = prisdetaljer_section.find('div', attrs={'data-testid': 'pricing-incicative-price'})
            self.prisantydning = int(''.join(filter(str.isdigit, prisantydning_section.find('span', class_="text-28 font-bold").text.strip())))

            # Finn totalpris
            totalpris_div = prisdetaljer_list.find('div', attrs={'data-testid': 'pricing-total-price'})
            self.totalpris = int(''.join(filter(str.isdigit, totalpris_div.find('dd').text.strip())))
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error extracting price data. Exception thrown: ", str(e))))

        #Extract common monthly cost
        try:
            # Finn felleskostnader
            felleskostnad_div = prisdetaljer_list.find('div', attrs={'data-testid': 'pricing-common-monthly-cost'})
            self.felleskostnader = int(''.join(filter(str.isdigit, felleskostnad_div.find('dd').text.strip())))
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error extracting 'felleskostnader'. Exception thrown: ", str(e))))

        #Extract municipal fees
        try:
            # Finn kommunale avgifter
            kommunale_avgifter_div = prisdetaljer_list.find('div', attrs={'data-testid': 'pricing-municipal-fees'})
            self.kommunaleAvgifter = int(''.join(filter(str.isdigit, kommunale_avgifter_div.find('dd').text.strip())))
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error extracting 'kommunale avgifter'. Exception thrown: ", str(e)))) 

        #Finn URL til eierhistorie siden
        try:
            eierhistorie_div = prisdetaljer_section.find('div', attrs={'data-testid': 'pricing-links'})
            self.eierhistorie_URL = "".join(("https://www.finn.no", eierhistorie_div.find('a', attrs={'data-testid': 'ownership-history-link'}, href=True)['href']))
            
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error extracting URL for ownership history. Exception thrown: ", str(e))))

        #extract key info
        #Finn n�kkelinfo hoved tags
        try:
            nokkelinfo_section = tag3.find('section', attrs={'data-testid': 'key-info'})
            nokkelinfo_list = nokkelinfo_section.find('dl')
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error extracting main tags for 'key info'. Exception thrown: ", str(e))))

        #Finn boligtype
        try:
            boligtype_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-property-type'})
            self.boligtype = boligtype_div.find('dd').text.strip()
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error finding 'Boligtype'. Exception thrown: ", str(e))))

        #Finn eierform
        try:
            eierform_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-ownership-type'})
            self.eierform = eierform_div.find('dd').text.strip()
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error finding 'eierform'. Exception thrown: ", str(e))))

        #Finn antall soverom
        try:
            soverom_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-bedrooms'})
            self.soverom = soverom_div.find('dd').text.strip()
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error finding 'Antall soverom'. Exception thrown: ", str(e))))

        #Finn prim�rrom
        try:
            primerrom_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-primary-area'})
            self.primerrom = int(primerrom_div.find('dd').text.strip()[:-2])
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error finding 'Primærrom'. Exception thrown: ", str(e))))

        #Finn bruksareal
        try:
            bruksareal_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-usable-area'})
            self.bruksareal = int(bruksareal_div.find('dd').text.strip()[:-2])
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error finding 'Bruksareal'. Exception thrown: ", str(e))))

        #Finn etasje
        try:
            etasje_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-floor'})
            self.etasje = int(''.join(filter(str.isdigit, etasje_div.find('dd').text.strip())))
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error finding 'Etasje'. Exception thrown: ", str(e))))

        #Finn bygge�r
        try:
            byggear_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-construction-year'})
            self.byggear = int(''.join(filter(str.isdigit, byggear_div.find('dd').text.strip())))
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error finding 'Byggeår'. Exception thrown: ", str(e))))

        #Finn energimerke
        try:
            energimerke_div = nokkelinfo_list.find('div', attrs={'data-testid': 'energy-label'})
            self.energimerke = energimerke_div.find('dd').text.strip()
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error finding 'Energimerke'. Exception thrown: ", str(e))))

        #Finn antall rom
        try:
            rom_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-rooms'})
            self.rom = int(''.join(filter(str.isdigit, rom_div.find('dd').text.strip())))
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error finding 'Antall rom'. Exception thrown: ", str(e))))

        #Finn Tomteareal
        try:
            plotArea_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-plot-area'})
            self.plotArea = int(''.join(filter(str.isdigit, plotArea_div.find('dd').text.strip()[:-9])))
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error finding 'Tomteareal'. Exception thrown: ", str(e))))

        #Finn fasiliteter hoved tags
        try:
            facilities_section = tag3.find('section', attrs={'data-testid': 'object-facilities'})
            facilities_list = facilities_section.find('div')
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error finding main tags for 'Fasiliteter'. Exception thrown: ", str(e))))

        #Extract data
        try:
            #extract parking and balcony
            facilities = facilities_list.find_all('div')
            for i in facilities:
                facility = i.text.strip()
                if str(facility) == 'Balkong/Terrasse':
                    self.balkong = 'Ja'
                elif str(facility) == 'Garasje/P-plass':
                    self.parkering = 'Ja'
            if self.balkong == None:
                self.balkong = 'Nei'
            if self.parkering == None:
                self.parkering = 'Nei'
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error extracting balcony and parking data. Exception thrown: ", str(e))))

        #Finn "Om boligen"
        try:
            description_section = tag3.find('section', attrs={'data-testid': 'about-property'})
            description_div = description_section.find('div', class_={'description-area whitespace-pre-wrap'})
            self.beskrivelse = description_div.get_text("\n", strip=True)
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error finding main tags for 'Fasiliteter'. Exception thrown: ", str(e))))        


    def extract_ownership_history(self, URL):
        #Parse page
        try:
            Get_page = SESSION.get(URL)
            ownership_history_page = BeautifulSoup(Get_page.content, 'html.parser')
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error parsing ownership history page. Exception thrown: ", str(e))))

        #Find tags containing all relevant data and extract data
        try:
            tag1 = ownership_history_page.find('main')
            tag2 = tag1.find('table')
            tag3 = tag2.find('tbody')
            tag4 = tag3.find_all('tr')

            #Retrieve all previous purchase data
            for i in range(0, len(tag4)):            
                tag5 = tag4[i].find_all('td')
                kjopsdato = tag5[0].text.strip()
                kjopssum = int(''.join(filter(str.isdigit, tag5[len(tag5)-1].text.strip())))
                kjopsdata = [kjopsdato, kjopssum]
                self.Tidligerekjop.append(kjopsdata)
        except Exception as e:
            logging.info("".join((self.progress, " - ", "Error extracting ownership history. Exception thrown: ", str(e))))

main()

