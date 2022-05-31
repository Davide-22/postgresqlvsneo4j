from traceback import print_tb
import psycopg2
from neo4j import GraphDatabase
import time

EX_NUMBER = 30
documentPostgreSQL = open("postgreSQL.txt", "w")
documentNeo4j = open("neo4j.txt", "w")

def writePostgresFile(cur,query):
    rows = cur.fetchall()
    documentPostgreSQL.write('Query {}: \n'.format(str(query)))
    for r in rows:
        if query == 5:
            documentPostgreSQL.write(str(r[0])+ ' ')
            documentPostgreSQL.write(str(r[1])+ '\n')
        else:
            documentPostgreSQL.write(str(r[0])+ '\n')
    documentPostgreSQL.write('\n')

def writeNeo4jFile(key,result, query, flag=True):
    documentNeo4j.write('Query {}: \n'.format(str(query)))

    if flag:
        for i in result:
            for j in key:
                documentNeo4j.write(str(i[j]))
                if j != len(key)-1:
                    documentNeo4j.write(" ") 
            documentNeo4j.write('\n')
    else:
        documentNeo4j.write(str(result.single()[key[0]]))
        documentNeo4j.write('\n')
    documentNeo4j.write('\n')
    
def postgresQueries(query):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="dmproj",
            user="postgres",
            password="postgres")
        cur = conn.cursor()
        if query == 1:
            # Ritorna tutti nomi degli atenei.
            q="""SELECT nomeesteso 
                FROM atenei"""
            for i in range(EX_NUMBER+1):
                if i == 1:
                    ts = time.time()
                cur.execute(q)
            print(f"Query 1 PostgreSQL execution time: {(time.time() - ts)*1000} ms")
            writePostgresFile(cur,query)
        if query == 2:
            #Ritorna tutti nomi degli atenei non statali ordinati per zona geografica, in ordine ascendente.
            q="""SELECT nomeesteso 
                FROM atenei 
                WHERE statale_nonstatale = 'Statale' 
                ORDER BY zonageografica ASC"""
            for i in range(EX_NUMBER+1):
                if i == 1:
                    ts = time.time()
                cur.execute(q)
            print(f"Query 2 PostgreSQL execution time: {(time.time() - ts)*1000} ms")
            writePostgresFile(cur,query)  
        if query == 3:
            #Ritorna la somma di tutti i laureati nel 2020.
            q="""SELECT SUM(numlaureati) 
                FROM laureati 
                WHERE anno=2020 """
            for i in range(EX_NUMBER+1):
                if i == 1:
                    ts = time.time()
                cur.execute(q)
            print(f"Query 3 PostgreSQL execution time: {(time.time() - ts)*1000} ms")
            writePostgresFile(cur,query)
        if query == 4:
            #Ritorna la somma dei laureati nel 2018 e nel 2019, nelle università del SUD.
            q="""SELECT SUM(l.numlaureati) 
                FROM laureati l, atenei a 
                WHERE (l.anno=2019 OR l.anno=2018) AND a.zonageografica='SUD' AND l.codateneo=a.cod"""
            for i in range(EX_NUMBER+1):
                if i == 1:
                    ts = time.time()
                cur.execute(q) 
            print(f"Query 4 PostgreSQL execution time: {(time.time() - ts)*1000} ms")
            writePostgresFile(cur,query)
        if query == 5:
            #Ritorna il codice ed il nome delle università con dimensione maggiore di 60000.
            q="""SELECT cod,nomeesteso
                FROM atenei
                WHERE atenei.dimensione = '60.000 e oltre'"""
            for i in range(EX_NUMBER+1):
                if i == 1:
                    ts = time.time()
                cur.execute(q)
            print(f"Query 5 PostgreSQL execution time: {(time.time() - ts)*1000} ms")
            writePostgresFile(cur,query)
        if query == 6:
            #Ritorna il numero delle università raggruppate per regione.
            q="""SELECT COUNT(nomeesteso)
                FROM atenei
                GROUP BY regione"""
            for i in range(EX_NUMBER+1):
                if i == 1:
                    ts = time.time()
                cur.execute(q)
            print(f"Query 6 PostgreSQL execution time: {(time.time() - ts)*1000} ms")
            writePostgresFile(cur,query)
        if query == 7:
            #Ritorna il massimo numero di laureati nel 2021, nelle università statali.
            q="""SELECT MAX(ab.sum)
                FROM (SELECT l.codateneo, SUM(l.numlaureati)
                        FROM laureati l, atenei a
                        WHERE l.anno = 2021 and a.statale_nonstatale='Statale' and l.codateneo = a.cod
                        GROUP BY l.codateneo) ab
                        """
            for i in range(EX_NUMBER+1):
                if i == 1:
                    ts = time.time()
                cur.execute(q)
            print(f"Query 7 PostgreSQL execution time: {(time.time() - ts)*1000} ms")
            writePostgresFile(cur,query)
        if query == 8:
            #Ritorna quanti maschi si sono laureati al politecnico di Milano nel 2015.
            q="""SELECT numlaureati
                FROM laureati
                WHERE anno=2015 AND sesso='M' AND nomeateneo='Milano Politecnico'"""
            for i in range(EX_NUMBER+1):
                if i == 1:
                    ts = time.time()
                cur.execute(q)
            print(f"Query 8 PostgreSQL execution time: {(time.time() - ts)*1000} ms")
            writePostgresFile(cur,query)
        if query == 9:
            #Ritorna la media delle femmine laureate alla Sapienza dal 2010 al 2021.
            q="""SELECT AVG(numlaureati)
                FROM laureati
                WHERE anno BETWEEN 2010 AND 2021 AND nomeateneo='Roma La Sapienza'"""
            for i in range(EX_NUMBER+1):
                if i == 1:
                    ts = time.time()
                cur.execute(q)
            print(f"Query 9 PostgreSQL execution time: {(time.time() - ts)*1000} ms")
            writePostgresFile(cur,query)
        if query == 10:
            #Ritorna il nomeesteso delle università che, nel 2021, hanno avuto un numero di laureati maggiore di 1000.
            q="""SELECT ab.nomeesteso
                FROM (SELECT l.codateneo, SUM(l.numlaureati), a.nomeesteso
                        FROM laureati l, atenei a
                        WHERE l.anno = 2021 AND l.codateneo=a.cod
                        GROUP BY l.codateneo, a.nomeesteso) ab
                WHERE ab.sum > 1000"""
            for i in range(EX_NUMBER+1):
                if i == 1:
                    ts = time.time()
                cur.execute(q)
            print(f"Query 10 PostgreSQL execution time: {(time.time() - ts)*1000} ms")
            writePostgresFile(cur,query)
        if query == 11:
            #Ritorna il numero di laureati nella regione LOMBARDIA.
            q="""SELECT SUM(l.numlaureati)
                FROM laureati l, atenei a
                WHERE a.regione='LOMBARDIA' AND l.codateneo=a.cod"""
            for i in range(EX_NUMBER+1):
                if i == 1:
                    ts = time.time()
                cur.execute(q)
            print(f"Query 11 PostgreSQL execution time: {(time.time() - ts)*1000} ms")
            writePostgresFile(cur,query)
        cur.close()

    except Exception as e:
        print("Errore di connessione")
        print(e)

def neo4jQueries(query):
    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "12345678"))
    session = driver.session()
    if query == 1:
        # Ritorna tutti nomi degli atenei.
        
        q = """MATCH (a:ateneo) 
            RETURN a.nomeesteso"""
        avail = 0
        cons = 0
        for i in range(EX_NUMBER+1):
            result = session.run(q)
            if i != 0:
                avail+=result.consume().result_available_after
                cons+=result.consume().result_consumed_after
            else:
                writeNeo4jFile(["a.nomeesteso"],result,query)
        print(f"Query 1 Neo4j execution time: {avail+cons} ms")
        
    if query == 2:
        # Ritorna tutti nomi degli atenei non statali ordinati per zona geografica, in ordine ascendente.
        q= """MATCH(a:ateneo)
            WHERE a.statale_nonstatale = 'Statale'
            RETURN a.nomeesteso
            ORDER BY a.zonageografica ASC
            """
        avail = 0
        cons = 0
        for i in range(EX_NUMBER+1):
            result = session.run(q)
            if i != 0:
                avail+=result.consume().result_available_after
                cons+=result.consume().result_consumed_after
            else:
                writeNeo4jFile(["a.nomeesteso"],result,query)
        print(f"Query 2 Neo4j execution time: {avail+cons} ms")

    if query == 3:
        #Ritorna la somma di tutti i laureati nel 2020.
        
        q="""MATCH(l:laureato)
            WHERE l.anno=2020
            RETURN SUM(l.numlaureati)"""
        avail = 0
        cons = 0
        for i in range(EX_NUMBER+1):
            result = session.run(q)
            if i != 0:
                avail+=result.consume().result_available_after
                cons+=result.consume().result_consumed_after
            else:
                writeNeo4jFile(["SUM(l.numlaureati)"],result,query,False)
        print(f"Query 3 Neo4j execution time: {avail+cons} ms")

    if query == 4:
        #Ritorna la somma dei laureati nel 2018 e nel 2019, nelle università del SUD.
        
        q="""MATCH (l:laureato)-[r]-(a:ateneo)
            WHERE (l.anno=2019 OR l.anno=2018) AND a.zonageografica ='SUD'
            RETURN SUM(l.numlaureati)"""
        avail = 0
        cons = 0
        for i in range(EX_NUMBER+1):
            result = session.run(q)
            if i != 0:
                avail+=result.consume().result_available_after
                cons+=result.consume().result_consumed_after
            else:
                writeNeo4jFile(["SUM(l.numlaureati)"],result,query,False)
        print(f"Query 4 Neo4j execution time: {avail+cons} ms")
    
    if query == 5:
        #Ritorna il codice ed il nome delle università con dimensione maggiore di 60000.
        
        q="""MATCH(a:ateneo)
            WHERE a.dimensione = '60.000 e oltre'
            RETURN a.cod,a.nomeesteso"""
        avail = 0
        cons = 0
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i != 0:
                avail+=result.consume().result_available_after
                cons+=result.consume().result_consumed_after
            else:
                writeNeo4jFile(["a.cod","a.nomeesteso"],result,query)
        print(f"Query 5 Neo4j execution time: {avail+cons} ms")

    if query == 6:
        #Ritorna il numero delle università raggruppate per regione.
        
        q="""MATCH(a:ateneo)
            RETURN COUNT(a.nomeesteso), a.regione"""
        avail = 0
        cons = 0
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i != 0:
                avail+=result.consume().result_available_after
                cons+=result.consume().result_consumed_after
            else:
                writeNeo4jFile(["COUNT(a.nomeesteso)", "a.regione"],result,query)
        print(f"Query 6 Neo4j execution time: {avail+cons} ms")

    if query == 7:
        #Ritorna il massimo numero di laureati nel 2021, nelle università statali.
        
        q="""CALL{
                MATCH (l:laureato)-[r]-(a:ateneo)
                WHERE l.anno=2021 AND a.statale_nonstatale='Statale'
                RETURN l.codateneo, SUM(l.numlaureati) AS sum
            }
            RETURN MAX(sum)"""
        avail = 0
        cons = 0
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i != 0:
                avail+=result.consume().result_available_after
                cons+=result.consume().result_consumed_after
            else:
                writeNeo4jFile(["MAX(sum)"],result,query,False)
        print(f"Query 7 Neo4j execution time: {avail+cons} ms")

    if query == 8:
        #Ritorna quanti maschi si sono laureati al politecnico di Milano nel 2015.
        
        q="""MATCH(l:laureato)
            WHERE l.anno=2015 AND l.sesso='M' AND l.nomeateneo='Milano Politecnico'
            RETURN l.numlaureati"""
        avail = 0
        cons = 0
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i != 0:
                avail+=result.consume().result_available_after
                cons+=result.consume().result_consumed_after
            else:
                writeNeo4jFile(["l.numlaureati"],result,query,False)
        print(f"Query 8 Neo4j execution time: {avail+cons} ms")

    if query == 9:
        #Ritorna la media delle femmine laureate alla Sapienza dal 2010 al 2021.
        
        q="""MATCH(l:laureato)
            WHERE l.anno>=2010 AND l.anno<=2021 AND l.nomeateneo='Roma La Sapienza'
            RETURN AVG(l.numlaureati)"""
        avail = 0
        cons = 0
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i != 0:
                avail+=result.consume().result_available_after
                cons+=result.consume().result_consumed_after
            else:
                writeNeo4jFile(["AVG(l.numlaureati)"],result,query,False)
        print(f"Query 9 Neo4j execution time: {avail+cons} ms")

    if query == 10:
        #Ritorna il nomeesteso delle università che, nel 2021, hanno avuto un numero di laureati maggiore di 1000.
        
        q="""CALL{
                MATCH (l:laureato)-[r]-(a:ateneo)
                WHERE l.anno=2021
                RETURN l.codateneo, a.nomeesteso AS nomeesteso, SUM(l.numlaureati) AS sum
        }
        WITH * WHERE sum>1000
        RETURN nomeesteso
        """
        avail = 0
        cons = 0
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i != 0:
                avail+=result.consume().result_available_after
                cons+=result.consume().result_consumed_after
            else:
                writeNeo4jFile(["nomeesteso"],result,query)
        print(f"Query 10 Neo4j execution time: {avail+cons} ms")

    if query == 11:
        #Ritorna il numero di laureati nella regione LOMBARDIA.
        
        q="""MATCH(l:laureato)-[r]-(a:ateneo)
            WHERE a.regione='LOMBARDIA'
            RETURN SUM(l.numlaureati)"""
        avail = 0
        cons = 0
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i != 0:
                avail+=result.consume().result_available_after
                cons+=result.consume().result_consumed_after
            else:
                writeNeo4jFile(["SUM(l.numlaureati)"],result,query,False)
        print(f"Query 11 Neo4j execution time: {avail+cons} ms")

    session.close()
    driver.close()

if __name__ == "__main__":

    for i in range(1,12):
        postgresQueries(i)
        neo4jQueries(i)
    documentPostgreSQL.close()
    documentNeo4j.close()