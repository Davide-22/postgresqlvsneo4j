import psycopg2
from neo4j import GraphDatabase
import time

EX_NUMBER = 30

def postgresQueries():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="dmproj",
            user="postgres",
            password="postgres")
        cur = conn.cursor()
        # Ritorna tutti nomi degli atenei.
        ts = time.time()
        q="""SELECT nomeesteso 
            FROM atenei"""
        for i in range(EX_NUMBER):
            if i == 1:
                ts = time.time()
            cur.execute(q)  
        print(f"Query 1 PostgreSQL execution time: {(time.time() - ts)*1000} ms")

        #Ritorna tutti nomi degli atenei non statali ordinati per zona geografica, in ordine ascendente.
        ts = time.time()
        q="""SELECT nomeesteso 
            FROM atenei 
            WHERE statale_nonstatale = 'Statale' 
            ORDER BY zonageografica ASC"""
        cur.execute(q)
        print(f"Query 2 PostgreSQL execution time: {(time.time() - ts)*1000} ms")

        #Ritorna la somma di tutti i laureati nel 2020.
        ts = time.time()
        q="""SELECT SUM(numlaureati) 
            FROM laureati 
            WHERE anno=2020 """
        cur.execute(q)
        print(f"Query 3 PostgreSQL execution time: {(time.time() - ts)*1000} ms")


        #Ritorna la somma dei laureati nel 2018 e nel 2019, nelle università del SUD.
        ts = time.time()
        q="""SELECT SUM(laureati.numlaureati) 
            FROM laureati JOIN atenei ON atenei.cod = laureati.codateneo 
            WHERE laureati.anno=2019 OR laureati.anno=2018 AND atenei.zonageografica='SUD'"""
        cur.execute(q)


        print(f"Query 4 PostgreSQL execution time: {(time.time() - ts)*1000} ms")

        #Ritorna il codice ed il nome delle università con dimensione maggiore di 60000.
        ts = time.time()
        q="""SELECT cod,nomeesteso
            FROM atenei
            WHERE atenei.dimensione = '60.000 e oltre'"""
        cur.execute(q)
        print(f"Query 5 PostgreSQL execution time: {(time.time() - ts)*1000} ms") 

        #Ritorna il numero delle università raggruppate per regione.
        ts = time.time()
        q="""SELECT COUNT(nomeesteso)
            FROM atenei
            GROUP BY regione"""
        cur.execute(q)
        print(f"Query 6 PostgreSQL execution time: {(time.time() - ts)*1000} ms")

        #Ritorna il massimo numero di laureati nel 2021, nelle università statali.
        ts = time.time()
        q=""""""

        cur.close()

    except Exception as e:
        print("Errore di connessione")
        print(e)

def neo4jQueries():
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "12345678"))
    session = driver.session()
    ts = time.time()
    q = "MATCH (n:ateneo) RETURN n.nomeesteso"
    result = session.run(q)
    avail = result._summary().result_available_after
    cons = result._summary().result_consumed_afterù
    print(avail,cons)
    print(f"Query 1 Neo4j execution time: {(time.time() - ts)*1000} ms")
    session.close()
    driver.close()

if __name__ == "__main__":
    postgresQueries()
    neo4jQueries()