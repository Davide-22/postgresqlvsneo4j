import psycopg2
from neo4j import GraphDatabase
import sys


if len(sys.argv) == 1:
    v = False
elif len(sys.argv) > 2 or sys.argv[1] != '-v':
    raise Exception("Wrong syntax")
else:
    v = True

EX_NUMBER = 30
documentPostgreSQL = open("postgreSQL.txt", "w")
documentNeo4j = open("neo4j.txt", "w")

postgresTimes = []
neo4jTimes = []       

def writePostgresFile(cur,query):
    rows = cur.fetchall()
    documentPostgreSQL.write('Query {}: \n'.format(str(query)))
    for r in rows:
        for i in range(len(r)):
            documentPostgreSQL.write(str(r[i])+ ' ')
        documentPostgreSQL.write('\n')
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

def printPerformance(planning_time,execution_time,query,dbms):
    if v:
        print(f"Query {query} {dbms}")
        print(f"Total execution time: {round(sum(execution_time),5)} ms")
        print(f"Average execution time: {round(sum(execution_time)/EX_NUMBER,5)} ms")
        print(f"Maximum execution time: {round(max(execution_time),5)} ms")
        print(f"Minimum execution time: {round(min(execution_time),5)} ms")
        print()
        print(f"Total planning time: {round(sum(planning_time),5)} ms")
        print(f"Average planning time: {round(sum(planning_time)/EX_NUMBER,5)} ms")
        print(f"Maximum planning time: {round(max(planning_time),5)} ms")
        print(f"Minimum planning time: {round(min(planning_time),5)} ms")
        print()
        print(f"Total time: {round(sum(planning_time)+sum(execution_time),5)} ms")
        print(f"Average time: {round((sum(planning_time)+sum(execution_time))/EX_NUMBER,5)} ms")
        print(f"Maximum time: {round(max(planning_time)+max(execution_time),5)} ms")
        print(f"Minimum time: {round(min(planning_time)+min(execution_time),5)} ms")
        print()
        print()
    else:
        print(f"Query {query} {dbms}")
        print(f"Total time: {round(sum(planning_time)+sum(execution_time),5)} ms")
        print(f"Average time: {round(sum(execution_time)/EX_NUMBER,5)} ms")
        print()
    if dbms == "PostgreSQL":
        postgresTimes.append(sum(planning_time)+sum(execution_time))
    if dbms == "Neo4j":
        neo4jTimes.append(sum(planning_time)+sum(execution_time))

def printDifference():
    for i in range(11):
        if postgresTimes[i] < neo4jTimes[i]:
            print(f"Query {i+1}: PostgreSQL was {round(neo4jTimes[i]/postgresTimes[i],5)} times faster than Neo4j")
        else:
            print(f"Query {i+1}: Neo4j was {round(postgresTimes[i]/neo4jTimes[i],5)} times faster than PostgreSQL")

def postgresQueries(query):
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="dmproj",
            user="postgres",
            password="postgres")
        cur = conn.cursor()
        if query == 1:
            #Return the "nomeesteso" of all the Universities.
            q="""EXPLAIN (ANALYZE, FORMAT 'json') 
                SELECT nomeesteso 
                FROM atenei"""
            execution_time = []
            planning_time = []
            for i in range(EX_NUMBER+1):
                cur.execute(q)
                result = cur.fetchall()
                planning_time.append(result[0][0][0]['Planning Time'])
                execution_time.append(result[0][0][0]['Execution Time'])
            printPerformance(planning_time,execution_time,1,"PostgreSQL")

            q="""SELECT nomeesteso 
                FROM atenei"""
            cur.execute(q)
            writePostgresFile(cur,query)
        if query == 2:
            #Return the "nomeesteso" of all the non  statal Universities ordered by "zonageografica", in ascending order.
            q="""EXPLAIN (ANALYZE, FORMAT 'json') 
                SELECT nomeesteso 
                FROM atenei 
                WHERE statale_nonstatale = 'Statale' 
                ORDER BY zonageografica ASC"""
            execution_time = []
            planning_time = []
            for i in range(EX_NUMBER+1):
                cur.execute(q)
                result = cur.fetchall()
                planning_time.append(result[0][0][0]['Planning Time'])
                execution_time.append(result[0][0][0]['Execution Time'])
            printPerformance(planning_time,execution_time,2,"PostgreSQL")
            q=""" SELECT nomeesteso 
                FROM atenei 
                WHERE statale_nonstatale = 'Statale' 
                ORDER BY zonageografica ASC"""
            cur.execute(q)
            writePostgresFile(cur,query)  
        if query == 3:
            #Return the sum of all the gratuated of 2020.
            q="""EXPLAIN (ANALYZE, FORMAT 'json') 
                SELECT SUM(numlaureati) 
                FROM laureati 
                WHERE anno=2020 """
            execution_time = []
            planning_time = []
            for i in range(EX_NUMBER+1):
                cur.execute(q)
                result = cur.fetchall()
                planning_time.append(result[0][0][0]['Planning Time'])
                execution_time.append(result[0][0][0]['Execution Time'])
            printPerformance(planning_time,execution_time,3,"PostgreSQL")
            q="""SELECT SUM(numlaureati) 
                FROM laureati 
                WHERE anno=2020 """
            cur.execute(q)
            writePostgresFile(cur,query)
        if query == 4:
            #Return the sum of graduated in 2018 and in 2019, in the southern Universities.
            q="""EXPLAIN (ANALYZE, FORMAT 'json') 
                SELECT SUM(l.numlaureati) 
                FROM laureati l, atenei a 
                WHERE (l.anno=2019 OR l.anno=2018) AND a.zonageografica='SUD' AND l.codateneo=a.cod"""
            execution_time = []
            planning_time = []
            for i in range(EX_NUMBER+1):
                cur.execute(q)
                result = cur.fetchall()
                planning_time.append(result[0][0][0]['Planning Time'])
                execution_time.append(result[0][0][0]['Execution Time'])
            printPerformance(planning_time,execution_time,4,"PostgreSQL")
            q="""SELECT SUM(l.numlaureati) 
                FROM laureati l, atenei a 
                WHERE (l.anno=2019 OR l.anno=2018) AND a.zonageografica='SUD' AND l.codateneo=a.cod"""
            cur.execute(q)
            writePostgresFile(cur,query)
        if query == 5:
            #Return the code and the name of Universities with dimension "maggiore di 60000".
            q="""EXPLAIN (ANALYZE, FORMAT 'json') 
                SELECT cod,nomeesteso
                FROM atenei
                WHERE atenei.dimensione = '60.000 e oltre'"""
            execution_time = []
            planning_time = []
            for i in range(EX_NUMBER+1):
                cur.execute(q)
                result = cur.fetchall()
                planning_time.append(result[0][0][0]['Planning Time'])
                execution_time.append(result[0][0][0]['Execution Time'])
            printPerformance(planning_time,execution_time,5,"PostgreSQL")
            q="""SELECT cod,nomeesteso
                FROM atenei
                WHERE atenei.dimensione = '60.000 e oltre'"""
            cur.execute(q)
            writePostgresFile(cur,query)
        if query == 6:
            #Return the number of Universities grouped by regions
            q="""EXPLAIN (ANALYZE, FORMAT 'json') 
                SELECT COUNT(nomeesteso), regione
                FROM atenei
                GROUP BY regione"""
            execution_time = []
            planning_time = []
            for i in range(EX_NUMBER+1):
                cur.execute(q)
                result = cur.fetchall()
                planning_time.append(result[0][0][0]['Planning Time'])
                execution_time.append(result[0][0][0]['Execution Time'])
            printPerformance(planning_time,execution_time,6,"PostgreSQL")
            q="""SELECT COUNT(nomeesteso), regione
                FROM atenei
                GROUP BY regione"""
            cur.execute(q)
            writePostgresFile(cur,query)
        if query == 7:
            #Return the maximum number of graduates in 2021, in the statal Universities
            q="""EXPLAIN (ANALYZE, FORMAT 'json') 
                SELECT MAX(ab.sum)
                FROM (SELECT l.codateneo, SUM(l.numlaureati)
                        FROM laureati l, atenei a
                        WHERE l.anno = 2021 and a.statale_nonstatale='Statale' and l.codateneo = a.cod
                        GROUP BY l.codateneo) ab
                        """
            execution_time = []
            planning_time = []
            for i in range(EX_NUMBER+1):
                cur.execute(q)
                result = cur.fetchall()
                planning_time.append(result[0][0][0]['Planning Time'])
                execution_time.append(result[0][0][0]['Execution Time'])
            printPerformance(planning_time,execution_time,7,"PostgreSQL")
            q="""SELECT MAX(ab.sum)
                FROM (SELECT l.codateneo, SUM(l.numlaureati)
                        FROM laureati l, atenei a
                        WHERE l.anno = 2021 and a.statale_nonstatale='Statale' and l.codateneo = a.cod
                        GROUP BY l.codateneo) ab
                        """
            cur.execute(q)
            writePostgresFile(cur,query)
        if query == 8:
            #Return how many males graduated from the "politecnico di Milano" in 2015.
            q="""EXPLAIN (ANALYZE, FORMAT 'json') 
                SELECT numlaureati
                FROM laureati
                WHERE anno=2015 AND sesso='M' AND nomeateneo='Milano Politecnico'"""
            execution_time = []
            planning_time = []
            for i in range(EX_NUMBER+1):
                cur.execute(q)
                result = cur.fetchall()
                planning_time.append(result[0][0][0]['Planning Time'])
                execution_time.append(result[0][0][0]['Execution Time'])
            printPerformance(planning_time,execution_time,8,"PostgreSQL")
            q="""SELECT numlaureati
                FROM laureati
                WHERE anno=2015 AND sesso='M' AND nomeateneo='Milano Politecnico'"""
            cur.execute(q)
            writePostgresFile(cur,query)
        if query == 9:
            #Return the average number of females graduated at Sapienza from 2010 to 2021.
            q="""EXPLAIN (ANALYZE, FORMAT 'json') 
                SELECT AVG(numlaureati)
                FROM laureati
                WHERE anno BETWEEN 2010 AND 2021 AND nomeateneo='Roma La Sapienza'"""
            execution_time = []
            planning_time = []
            for i in range(EX_NUMBER+1):
                cur.execute(q)
                result = cur.fetchall()
                planning_time.append(result[0][0][0]['Planning Time'])
                execution_time.append(result[0][0][0]['Execution Time'])
            printPerformance(planning_time,execution_time,9,"PostgreSQL")
            q="""SELECT AVG(numlaureati)
                FROM laureati
                WHERE anno BETWEEN 2010 AND 2021 AND nomeateneo='Roma La Sapienza'"""
            cur.execute(q)
            writePostgresFile(cur,query)
        if query == 10:
            #Return the "nomeesteso" of Universities which, in 2021, had a number of graduates greater than 1000.
            q="""EXPLAIN (ANALYZE, FORMAT 'json') 
                SELECT ab.nomeesteso
                FROM (SELECT l.codateneo, SUM(l.numlaureati), a.nomeesteso
                        FROM laureati l, atenei a
                        WHERE l.anno = 2021 AND l.codateneo=a.cod
                        GROUP BY l.codateneo, a.nomeesteso) ab
                WHERE ab.sum > 1000"""
            execution_time = []
            planning_time = []
            for i in range(EX_NUMBER+1):
                cur.execute(q)
                result = cur.fetchall()
                planning_time.append(result[0][0][0]['Planning Time'])
                execution_time.append(result[0][0][0]['Execution Time'])
            printPerformance(planning_time,execution_time,10,"PostgreSQL")
            q="""SELECT ab.nomeesteso
                FROM (SELECT l.codateneo, SUM(l.numlaureati), a.nomeesteso
                        FROM laureati l, atenei a
                        WHERE l.anno = 2021 AND l.codateneo=a.cod
                        GROUP BY l.codateneo, a.nomeesteso) ab
                WHERE ab.sum > 1000"""
            cur.execute(q)
            writePostgresFile(cur,query)
        if query == 11:
            #Return the number of graduates in the "Lombardia" region
            q="""EXPLAIN (ANALYZE, FORMAT 'json') 
                SELECT SUM(l.numlaureati)
                FROM laureati l, atenei a
                WHERE a.regione='LOMBARDIA' AND l.codateneo=a.cod"""
            execution_time = []
            planning_time = []
            for i in range(EX_NUMBER+1):
                cur.execute(q)
                result = cur.fetchall()
                planning_time.append(result[0][0][0]['Planning Time'])
                execution_time.append(result[0][0][0]['Execution Time'])
            printPerformance(planning_time,execution_time,11,"PostgreSQL")
            q="""SELECT SUM(l.numlaureati)
                FROM laureati l, atenei a
                WHERE a.regione='LOMBARDIA' AND l.codateneo=a.cod"""
            cur.execute(q)
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
        # Return the "nomeesteso" of all the Universities.
        
        q = """MATCH (a:ateneo) 
            RETURN a.nomeesteso"""
        execution_time = []
        planning_time = []
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i == EX_NUMBER-1:
                writeNeo4jFile(["a.nomeesteso"],result,query)
            planning_time.append(result.consume().result_available_after)
            execution_time.append(result.consume().result_consumed_after)
        printPerformance(planning_time,execution_time,1,"Neo4j")
        
    if query == 2:
        # Return the "nomeesteso" of all the non  statal Universities ordered by "zonageografica", in ascending order.
        q= """MATCH(a:ateneo)
            WHERE a.statale_nonstatale = 'Statale'
            RETURN a.nomeesteso
            ORDER BY a.zonageografica ASC
            """
        execution_time = []
        planning_time = []
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i == EX_NUMBER-1:
                writeNeo4jFile(["a.nomeesteso"],result,query)
            planning_time.append(result.consume().result_available_after)
            execution_time.append(result.consume().result_consumed_after)
        printPerformance(planning_time,execution_time,2,"Neo4j")

    if query == 3:
        #Return the sum of all the gratuated of 2020.
        
        q="""MATCH(l:laureato)
            WHERE l.anno=2020
            RETURN SUM(l.numlaureati)"""
        execution_time = []
        planning_time = []
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i == EX_NUMBER-1:
                writeNeo4jFile(["SUM(l.numlaureati)"],result,query,False)
            planning_time.append(result.consume().result_available_after)
            execution_time.append(result.consume().result_consumed_after)
        printPerformance(planning_time,execution_time,3,"Neo4j")

    if query == 4:
        #Return the sum of graduated in 2018 and in 2019, in the southern Universities.
        
        q="""MATCH (l:laureato)-[r]-(a:ateneo)
            WHERE (l.anno=2019 OR l.anno=2018) AND a.zonageografica ='SUD'
            RETURN SUM(l.numlaureati)"""
        execution_time = []
        planning_time = []
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i == EX_NUMBER-1:
                writeNeo4jFile(["SUM(l.numlaureati)"],result,query,False)
            planning_time.append(result.consume().result_available_after)
            execution_time.append(result.consume().result_consumed_after)
        printPerformance(planning_time,execution_time,4,"Neo4j")
    
    if query == 5:
        #Return the code and name of Universities with dimension "maggiore di 60000".
        
        q="""MATCH(a:ateneo)
            WHERE a.dimensione = '60.000 e oltre'
            RETURN a.cod,a.nomeesteso"""
        execution_time = []
        planning_time = []
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i == EX_NUMBER-1:
                writeNeo4jFile(["a.cod","a.nomeesteso"],result,query)
            planning_time.append(result.consume().result_available_after)
            execution_time.append(result.consume().result_consumed_after)
        printPerformance(planning_time,execution_time,5,"Neo4j")

    if query == 6:
        #Return the number of Universities grouped by regions
        
        q="""MATCH(a:ateneo)
            RETURN COUNT(a.nomeesteso), a.regione"""
        execution_time = []
        planning_time = []
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i == EX_NUMBER-1:
                writeNeo4jFile(["COUNT(a.nomeesteso)", "a.regione"],result,query)
            planning_time.append(result.consume().result_available_after)
            execution_time.append(result.consume().result_consumed_after)
        printPerformance(planning_time,execution_time,6,"Neo4j")

    if query == 7:
        #Return the maximum number of graduates in 2021, in the statal Universities
        
        q="""CALL{
                MATCH (l:laureato)-[r]-(a:ateneo)
                WHERE l.anno=2021 AND a.statale_nonstatale='Statale'
                RETURN l.codateneo, SUM(l.numlaureati) AS sum
            }
            RETURN MAX(sum)"""
        execution_time = []
        planning_time = []
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i == EX_NUMBER-1:
                writeNeo4jFile(["MAX(sum)"],result,query,False)
            planning_time.append(result.consume().result_available_after)
            execution_time.append(result.consume().result_consumed_after)
        printPerformance(planning_time,execution_time,7,"Neo4j")

    if query == 8:
        #Return how many males graduated from the "politecnico di Milano" in 2015.
        
        q="""MATCH(l:laureato)
            WHERE l.anno=2015 AND l.sesso='M' AND l.nomeateneo='Milano Politecnico'
            RETURN l.numlaureati"""
        execution_time = []
        planning_time = []
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i == EX_NUMBER-1:
                writeNeo4jFile(["l.numlaureati"],result,query,False)
            planning_time.append(result.consume().result_available_after)
            execution_time.append(result.consume().result_consumed_after)
        printPerformance(planning_time,execution_time,8,"Neo4j")

    if query == 9:
        #Return the average number of females graduated at Sapienza from 2010 to 2021.
        
        q="""MATCH(l:laureato)
            WHERE l.anno>=2010 AND l.anno<=2021 AND l.nomeateneo='Roma La Sapienza'
            RETURN AVG(l.numlaureati)"""
        execution_time = []
        planning_time = []
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i == EX_NUMBER-1:
                writeNeo4jFile(["AVG(l.numlaureati)"],result,query,False)
            planning_time.append(result.consume().result_available_after)
            execution_time.append(result.consume().result_consumed_after)
        printPerformance(planning_time,execution_time,9,"Neo4j")

    if query == 10:
        #Return the "nomeesteso" of Universities which, in 2021, had a number of graduates greater than 1000.
        
        q="""CALL{
                MATCH (l:laureato)-[r]-(a:ateneo)
                WHERE l.anno=2021
                RETURN l.codateneo, a.nomeesteso AS nomeesteso, SUM(l.numlaureati) AS sum
        }
        WITH * WHERE sum>1000
        RETURN nomeesteso
        """
        execution_time = []
        planning_time = []
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i == EX_NUMBER-1:
                writeNeo4jFile(["nomeesteso"],result,query)
            planning_time.append(result.consume().result_available_after)
            execution_time.append(result.consume().result_consumed_after)
        printPerformance(planning_time,execution_time,10,"Neo4j")

    if query == 11:
        #Return the number of graduates in the "Lombardia" region
        
        q="""MATCH(l:laureato)-[r]-(a:ateneo)
            WHERE a.regione='LOMBARDIA'
            RETURN SUM(l.numlaureati)"""
        execution_time = []
        planning_time = []
        for i in range(EX_NUMBER):
            result = session.run(q)
            if i == EX_NUMBER-1:
                writeNeo4jFile(["SUM(l.numlaureati)"],result,query,False)
            planning_time.append(result.consume().result_available_after)
            execution_time.append(result.consume().result_consumed_after)
        printPerformance(planning_time,execution_time,11,"Neo4j")

    session.close()
    driver.close()

if __name__ == "__main__":

    for i in range(1,12):
        postgresQueries(i)
        neo4jQueries(i)
    printDifference()
    documentPostgreSQL.close()
    documentNeo4j.close()