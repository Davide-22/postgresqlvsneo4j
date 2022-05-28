from importlib.resources import path
import psycopg2
from neo4j import GraphDatabase

def initPostgres():
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="dmproj",
            user="postgres",
            password="postgres")
        cur = conn.cursor()
        q = "DROP TABLE IF EXISTS atenei;"
        cur.execute(q)
        q = """
            CREATE TABLE atenei (
            _id int,
            cod int UNIQUE,
            nomeesteso VARCHAR(100),
            nomeoperativo VARCHAR(100),
            status VARCHAR(100),
            tipologia VARCHAR(100),
            statale_nonstatale VARCHAR(100),
            indirizzo VARCHAR(100),
            comune VARCHAR(100),
            provincia VARCHAR(100),
            regione VARCHAR(100),
            zonageografica VARCHAR(100),
            url VARCHAR(100),
            dimensione VARCHAR(100),
            PRIMARY KEY (_id)
            );"""
        cur.execute(q)
        path = '/home/davide/Scrivania/postgresqlvsneo4j/csv/Università.csv'
        q = """COPY atenei(_id, cod, nomeesteso, nomeoperativo,status,tipologia,statale_nonstatale,indirizzo,comune,provincia,regione,zonageografica,url,dimensione)
            FROM stdin
            DELIMITER ','
            CSV HEADER;"""
        with open(path, 'r') as f:
            cur.copy_expert(sql=q, file=f)
        #rows = cur.fetchall()
        #print(rows)   
        q = "DROP TABLE IF EXISTS laureati;"
        cur.execute(q) 
        q = """CREATE TABLE laureati (
            anno int,
            codateneo int,
            nomeateneo VARCHAR(100),
            sesso VARCHAR(1),
            numlaureati int,
            CONSTRAINT fk_laureati
                FOREIGN KEY(codateneo) 
                    REFERENCES atenei(cod)
                        ON DELETE CASCADE
            );"""
        cur.execute(q)
        path = '/home/davide/Scrivania/postgresqlvsneo4j/csv/Laureati.csv'
        q = """COPY laureati(anno,codateneo,nomeateneo,sesso,numlaureati)
            FROM stdin
            DELIMITER ';'
            CSV HEADER;"""
        with open(path, 'r') as f:
            cur.copy_expert(sql=q, file=f)
        conn.commit()
        cur.close()
    except Exception as e:
        print("Errore di connessione")
        print(e)



def initNeo4j():
    uri = "bolt://localhost:7687"
    #CREATE CONSTRAINT ON (ateneo:ateneo) ASSERT ateno._id IS UNIQUE
    driver = GraphDatabase.driver(uri, auth=("neo4j", "12345678"))
    session = driver.session()
    q = "MATCH (n) DETACH DELETE n"
    session.run(q)
    q = "DROP CONSTRAINT ON (ateneo:ateneo) ASSERT ateneo._id IS UNIQUE"
    session.run(q)
    q = "CREATE CONSTRAINT ON (ateneo:ateneo) ASSERT ateneo._id IS UNIQUE"
    session.run(q)
    q = """LOAD CSV FROM 'file:///Università.csv' AS line FIELDTERMINATOR ','
            CREATE (:ateneo {_id: toInteger(line[0]), cod: toInteger(line[1]), nomeesteso : line[2], nomeoperativo : line[3], status : line[4], tipologia : line[5], statale_nonstatale : line[6], indirizzo : line[7], comune : line[8], provincia : line[9], regione : line[10], zonageografica : line[11], url : line[12], dimensione : line[13]})"""
    session.run(q)

    q = """LOAD CSV FROM 'file:///Laureati.csv' AS line FIELDTERMINATOR ';'
            CREATE (:laureato {anno: toInteger(line[0]), codateneo: toInteger(line[1]), nomeateneo : line[2], sesso : line[3], numlaureati : toInteger(line[4])})"""
    session.run(q)
    q = """MATCH
        (l:laureato),
        (a:ateneo)
        WHERE a.cod = l.codateneo  
        CREATE (l)-[r:RELTYPE {name: 'laureati presso ' + a.nomeoperativo}]->(a)
        RETURN type(r)"""
    r = session.run(q)


    session.close()
    driver.close()

if __name__ == "__main__":
    initPostgres()
    initNeo4j()