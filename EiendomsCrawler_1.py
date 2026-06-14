import requests
from bs4 import BeautifulSoup
import mysql.connector
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import re
import Auxiliary
import json
import random

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
MAX_WORKERS = 80
sem = threading.Semaphore(100000)

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
                    try:
                        URL = "".join(("https://www.finn.no/realestate/homes/ad.html?finnkode=", str(i)))

                        #Append progression to config file
                        with CONFIG_LOCK:
                            Auxiliary.appendProgress(configData, useFile, URL, i)

                        # Extract and store data
                        extract(URL, configData, str(i))
                        time.sleep(random.uniform(0.01, 0.05))

                    except Exception as e:
                        logging.error(f"{i} - Error in worker: {e}")
                    finally:
                        sem.release()

                
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    #count = 0
                    for i in tqdm(range(start, finish + 1), total=finish - start + 1):
                        sem.acquire()
                        executor.submit(worker, i)
                        #count += 1
                        #if count >= finish - start + 1:
                        #    finished = True
                    finished = True

        if(not finished):
            time.sleep(6)
            connectionTries += 1
            if (connectionTries == 10):
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
 
def store(Boligdata, progress, configData):

    COLUMNS = [
        ("Finnkode", "BIGINT"),
        ("Link", "VARCHAR(255)"),
        ("Adresse", "VARCHAR(255)"),
        ("Prisantydning", "VARCHAR(255)"),
        ("Totalpris", "VARCHAR(255)"),
        ("Felleskostnader", "VARCHAR(255)"),
        ("KommunaleAvgifter", "VARCHAR(255)"),
        ("Boligtype", "VARCHAR(255)"),
        ("Eierform", "VARCHAR(255)"),
        ("AntallSoverom", "INT"),
        ("ArealPrimerrom", "INT"),
        ("Bruksareal", "INT"),
        ("Etasje", "VARCHAR(255)"),
        ("Byggear", "VARCHAR(255)"),
        ("Energimerke", "VARCHAR(255)"),
        ("AntallRom", "INT"),
        ("Parkering", "VARCHAR(5)"),
        ("Balkong", "VARCHAR(5)"),
        ("Tomteareal", "VARCHAR(255)"),
        ("Beskrivelse", "TEXT"),
        ("Tidligerekjøp", "TEXT"),
        ("SistEndret", "VARCHAR(255)"),
        ("Standard_Bad", "DECIMAL(2,1)"),
        ("Standard_Kjøkken", "DECIMAL(2,1)"),
        ("Standard_Bolig", "DECIMAL(2,1)"),
        ("SistEndretDT", "DATETIME"),
    ]

    table = "data"

    col_names = [n for n, _ in COLUMNS]
    col_defs = ", ".join(f"`{n}` {t}" for n, t in COLUMNS)

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
        f"VALUES ({', '.join(['%s'] * len(col_names))})"
    )

    # direct dict extraction (no lambdas, no getattr)
    values = (
        Boligdata.get("Finnkode"),
        Boligdata.get("Link"),
        Boligdata.get("Adresse"),

        Boligdata.get("Prisantydning"),
        Boligdata.get("Totalpris"),
        Boligdata.get("Felleskostnader"),
        Boligdata.get("KommunaleAvgifter"),

        Boligdata.get("Boligtype"),
        Boligdata.get("Eierform"),

        Boligdata.get("AntallSoverom"),
        Boligdata.get("ArealPrimerrom"),
        Boligdata.get("Bruksareal"),

        Boligdata.get("Etasje"),
        Boligdata.get("Byggear"),
        Boligdata.get("Energimerke"),

        Boligdata.get("AntallRom"),
        Boligdata.get("Parkering"),
        Boligdata.get("Balkong"),

        Boligdata.get("Tomteareal"),
        Boligdata.get("Beskrivelse"),

        json.dumps(Boligdata.get("Tidligerekjøp")) if Boligdata.get("Tidligerekjøp") is not None else None,
        Boligdata.get("SistEndret"),

        Boligdata.get("Standard_Bad"),
        Boligdata.get("Standard_Kjøkken"),
        Boligdata.get("Standard_Bolig"),

        Boligdata.get("SistEndretDT"),
    )

    mydb = mycursor = None
    try:
        mydb = mysql.connector.connect(
            host=configData.get("mysql", "host"),
            user=configData.get("mysql", "user"),
            password=configData.get("mysql", "password"),
            database=configData.get("mysql", "database"),
        )

        mycursor = mydb.cursor()
        mycursor.execute(create_sql)
        mycursor.execute(insert_sql, values)
        mydb.commit()

        logging.info(f"{progress} - {Boligdata.get('Finnkode')} successfully added to table.")

    except Exception as e:
        logging.error(f"{progress} - Error when writing to database: {e}")

    finally:
        if mycursor:
            mycursor.close()
        if mydb:
            mydb.close()


def extract(URL, configData, progress):

    def adpage_exists(adpage):
        
        try:
            # Positive check: a real ad page contains the detailed section or FINN-kode text.
            exists = (
                adpage.find("section", attrs={"aria-label": "Detaljert informasjon om bolig"}) is not None
                or adpage.find(string=re.compile(r"\bFINN-kode\b", re.I)) is not None
            )
            
            return exists
        except Exception as e:
            logging.error("".join((progress, " - ", "Error checking ad page existence. Exception thrown: ", str(e))))

    def extract_adpage(adpage):

        DBData = {
            "Finnkode": None,
            "Link": None,
            "Adresse": None,
            "Prisantydning": None,
            "Totalpris": None,
            "Felleskostnader": None,
            "KommunaleAvgifter": None,
            "Boligtype": None,
            "Eierform": None,
            "AntallSoverom": None,
            "ArealPrimerrom": None,
            "Bruksareal": None,
            "Etasje": None,
            "Byggear": None,
            "Energimerke": None,
            "AntallRom": None,
            "Parkering": None,
            "Balkong": None,
            "Tomteareal": None,
            "Beskrivelse": None,
            "Tidligerekjøp": [],
            "SistEndret": None,
            "Standard_Bad": None,
            "Standard_Kjøkken": None,
            "Standard_Bolig": None,
            "SistEndretDT": None,
        }

        DBData["Finnkode"] = int(URL.split("finnkode=")[1].split("&")[0])
        DBData["Link"] = URL

        #Find tags containing all relevant data
        try:
            tag1 = adpage.find('body')
            tag2 = tag1.find('main')
            tag3 = tag2.find('section', attrs={'aria-label': 'Detaljert informasjon om bolig'})
        except Exception as e:
            logging.error("".join((progress, " - ", "Error getting main tags for extraction. Exception thrown: ", str(e))))

        #Find sist endret and create a datetime version of it as well
        try:
            SistEndretMain = tag2.find('section', attrs={'aria-labelledby': 'ad-info-heading'})
            SistEndretList = SistEndretMain.find_all('tr')
            SistEndret = SistEndretList[1].find('td')
            DBData["SistEndret"] = SistEndret.text.strip()
            DBData["SistEndretDT"] = Auxiliary.parse_norwegian_date(DBData["SistEndret"])
        except Exception as e:
            logging.info("".join((progress, " - ", "Error extracting 'sist endret dato'. Exception thrown: ", str(e))))

        #Extract address
        try:
            address_section = tag3.find('section', attrs={'aria-label': 'Tittel'})
            DBData["Adresse"] = address_section.find('span', attrs={'data-testid': 'object-address'}).text.strip()
        except Exception as e:
            logging.info("".join((progress, " - ", "Error extracting address. Exception thrown: ", str(e))))

        #Extract price info
        try:
            prisdetaljer_section = tag3.find('section', attrs={'aria-label': 'Prisdetaljer'})
            prisdetaljer_list = prisdetaljer_section.find('dl')

            # Finn prisantydning
            prisantydning_section = prisdetaljer_section.find('div', attrs={'data-testid': 'pricing-incicative-price'})
            DBData["Prisantydning"] = int(''.join(filter(str.isdigit, prisantydning_section.find('span', class_="text-28 font-bold").text.strip())))

            # Finn totalpris
            totalpris_div = prisdetaljer_list.find('div', attrs={'data-testid': 'pricing-total-price'})
            DBData["Totalpris"] = int(''.join(filter(str.isdigit, totalpris_div.find('dd').text.strip())))
        except Exception as e:
            logging.info("".join((progress, " - ", "Error extracting price data. Exception thrown: ", str(e))))

        #Extract common monthly cost
        try:
            # Finn felleskostnader
            felleskostnad_div = prisdetaljer_list.find('div', attrs={'data-testid': 'pricing-common-monthly-cost'})
            DBData["Felleskostnader"] = int(''.join(filter(str.isdigit, felleskostnad_div.find('dd').text.strip())))
        except Exception as e:
            logging.info("".join((progress, " - ", "Error extracting 'felleskostnader'. Exception thrown: ", str(e))))

        #Extract municipal fees
        try:
            # Finn kommunale avgifter
            kommunale_avgifter_div = prisdetaljer_list.find('div', attrs={'data-testid': 'pricing-municipal-fees'})
            DBData["KommunaleAvgifter"] = int(''.join(filter(str.isdigit, kommunale_avgifter_div.find('dd').text.strip())))
        except Exception as e:
            logging.info("".join((progress, " - ", "Error extracting 'kommunale avgifter'. Exception thrown: ", str(e))))

        #Finn URL til eierhistorie siden
        try:
            eierhistorie_div = prisdetaljer_section.find('div', attrs={'data-testid': 'pricing-links'})
            eierhistorie_URL = "".join(("https://www.finn.no", eierhistorie_div.find('a', attrs={'data-testid': 'ownership-history-link'}, href=True)['href']))
            
        except Exception as e:
            logging.info("".join((progress, " - ", "Error extracting URL for ownership history. Exception thrown: ", str(e))))

        #extract key info
        #Finn n�kkelinfo hoved tags
        try:
            nokkelinfo_section = tag3.find('section', attrs={'data-testid': 'key-info'})
            nokkelinfo_list = nokkelinfo_section.find('dl')
        except Exception as e:
            logging.info("".join((progress, " - ", "Error extracting main tags for 'key info'. Exception thrown: ", str(e))))

        #Finn boligtype
        try:
            boligtype_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-property-type'})
            DBData["Boligtype"] = boligtype_div.find('dd').text.strip()
        except Exception as e:
            logging.info("".join((progress, " - ", "Error finding 'Boligtype'. Exception thrown: ", str(e))))

        #Finn eierform
        try:
            eierform_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-ownership-type'})
            DBData["Eierform"] = eierform_div.find('dd').text.strip()
        except Exception as e:
            logging.info("".join((progress, " - ", "Error finding 'eierform'. Exception thrown: ", str(e))))

        #Finn antall soverom
        try:
            soverom_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-bedrooms'})
            DBData["AntallSoverom"] = soverom_div.find('dd').text.strip()
        except Exception as e:
            logging.info("".join((progress, " - ", "Error finding 'Antall soverom'. Exception thrown: ", str(e))))

        #Finn prim�rrom
        try:
            primerrom_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-primary-area'})
            DBData["ArealPrimerrom"] = int(primerrom_div.find('dd').text.strip()[:-2])
        except Exception as e:
            logging.info("".join((progress, " - ", "Error finding 'Primærrom'. Exception thrown: ", str(e))))

        #Finn bruksareal
        try:
            bruksareal_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-usable-area'})
            DBData["Bruksareal"] = int(bruksareal_div.find('dd').text.strip()[:-2])
        except Exception as e:
            logging.info("".join((progress, " - ", "Error finding 'Bruksareal'. Exception thrown: ", str(e))))

        #Finn etasje
        try:
            etasje_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-floor'})
            DBData["Etasje"] = int(''.join(filter(str.isdigit, etasje_div.find('dd').text.strip())))
        except Exception as e:
            logging.info("".join((progress, " - ", "Error finding 'Etasje'. Exception thrown: ", str(e))))

        #Finn bygge�r
        try:
            byggear_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-construction-year'})
            DBData["Byggear"] = int(''.join(filter(str.isdigit, byggear_div.find('dd').text.strip())))
        except Exception as e:
            logging.info("".join((progress, " - ", "Error finding 'Byggeår'. Exception thrown: ", str(e))))

        #Finn energimerke
        try:
            energimerke_div = nokkelinfo_list.find('div', attrs={'data-testid': 'energy-label'})
            DBData["Energimerke"] = energimerke_div.find('dd').text.strip()
        except Exception as e:
            logging.info("".join((progress, " - ", "Error finding 'Energimerke'. Exception thrown: ", str(e))))

        #Finn antall rom
        try:
            rom_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-rooms'})
            DBData["AntallRom"] = int(''.join(filter(str.isdigit, rom_div.find('dd').text.strip())))
        except Exception as e:
            logging.info("".join((progress, " - ", "Error finding 'Antall rom'. Exception thrown: ", str(e))))

        #Finn Tomteareal
        try:
            plotArea_div = nokkelinfo_list.find('div', attrs={'data-testid': 'info-plot-area'})
            DBData["Tomteareal"] = int(''.join(filter(str.isdigit, plotArea_div.find('dd').text.strip()[:-9])))
        except Exception as e:
            logging.info("".join((progress, " - ", "Error finding 'Tomteareal'. Exception thrown: ", str(e))))

        #Finn fasiliteter hoved tags
        try:
            facilities_section = tag3.find('section', attrs={'data-testid': 'object-facilities'})
            facilities_list = facilities_section.find('div')
        except Exception as e:
            logging.info("".join((progress, " - ", "Error finding main tags for 'Fasiliteter'. Exception thrown: ", str(e))))

        #Extract data
        try:
            #extract parking and balcony
            facilities = facilities_list.find_all('div')
            for i in facilities:
                facility = i.text.strip()
                if str(facility) == 'Balkong/Terrasse':
                    DBData["Balkong"] = 'Ja'
                elif str(facility) == 'Garasje/P-plass':
                    DBData["Parkering"] = 'Ja'
            if DBData["Balkong"] == None:
                DBData["Balkong"] = 'Nei'
            if DBData["Parkering"] == None:
                DBData["Parkering"] = 'Nei'
        except Exception as e:
            logging.info("".join((progress, " - ", "Error extracting balcony and parking data. Exception thrown: ", str(e))))

        #Finn "Om boligen"
        try:
            description_section = tag3.find('section', attrs={'data-testid': 'about-property'})
            description_div = description_section.find('div', class_={'description-area whitespace-pre-wrap'})
            DBData["Beskrivelse"] = description_div.get_text("\n", strip=True)
        except Exception as e:
            logging.info("".join((progress, " - ", "Error finding main tags for 'Fasiliteter'. Exception thrown: ", str(e))))

        #Extract ownership history
        #Parse page
        try:
            Get_page = SESSION.get(eierhistorie_URL)
            ownership_history_page = BeautifulSoup(Get_page.content, 'html.parser')
            Get_page.close()
        except Exception as e:
            logging.info("".join((progress, " - ", "Error parsing ownership history page. Exception thrown: ", str(e))))

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
                DBData["Tidligerekjøp"].append(kjopsdata)
            ownership_history_page.decompose()
        except Exception as e:
            logging.info("".join((progress, " - ", "Error extracting ownership history. Exception thrown: ", str(e))))

        return DBData
    
    with SESSION.get(URL, timeout=10, stream=False) as web_page:
        adpage = BeautifulSoup(web_page.content, "html.parser")

    if(adpage_exists(adpage)): #returns true if the ad exists
        Boligdata = extract_adpage(adpage)

        adpage.decompose()
        del adpage
        #Store data if ad exists, passes the current instance to store class
        store(Boligdata, progress, configData)

        return
    adpage.decompose()
    return

main()

