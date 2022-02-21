from neo4j import GraphDatabase, basic_auth
import socket


class Movie_queries(object):
    def __init__(self, password):
        self.driver = GraphDatabase.driver("bolt://localhost", auth=("neo4j", password), encrypted=False)
        self.session = self.driver.session()
        self.transaction = self.session.begin_transaction()

    def q0(self):
        result = self.transaction.run("""
            MATCH (n:Actor) RETURN n.name, n.id ORDER BY n.birthday ASC LIMIT 3
        """)
        return [(r[0], r[1]) for r in result]

    def q1(self):
        result = self.transaction.run("""
            MATCH (a:Actor) -[:ACTS_IN] -> (m:Movie)
            RETURN a.name, count(*) as numMovies
            ORDER BY numMovies DESC, a.name ASC
            LIMIT 20
        """)
        return [(r[0], r[1]) for r in result]

    def q2(self):
        result = self.transaction.run("""
            MATCH (a:Actor) -[:ACTS_IN] -> (m:Movie), ()-[:RATED]->(m:Movie)
            WITH count(DISTINCT(a) -[:ACTS_IN] -> (m)) as cast, m as m
            RETURN m.title, cast
            ORDER BY cast DESC 
            LIMIT 1
        """)
        return [(r[0], r[1]) for r in result]

    def q3(self):
        result = self.transaction.run("""
            MATCH (d:Director) -[:DIRECTED] -> (m:Movie)
            WITH count(DISTINCT (m.genre)) as numGen, d as d
            WHERE numGen >= 2
            RETURN d.name, numGen
            ORDER BY numGen DESC, d.name ASC
        """)
        return [(r[0], r[1]) for r in result]

    def q4(self):
        result = self.transaction.run("""
        WITH ['Kevin Bacon'] as ls 
        MATCH (a:Actor {name: 'Kevin Bacon'}) -[:ACTS_IN] -> (m:Movie) <- [:ACTS_IN] - (othera:Actor)-[:ACTS_IN] -> (otherm:Movie) <- [:ACTS_IN] -(lasta:Actor)
        WHERE NOT othera.name in ls and not lasta.name in ls and not (a)-[:ACTS_IN]->()<-[:ACTS_IN]-(lasta)
        RETURN DISTINCT lasta.name
        ORDER BY lasta.name 
            
        """)
        return [(r[0]) for r in result]

if __name__ == "__main__":
    sol = Movie_queries("neo4jpass")
    print("---------- Q0 ----------")
    print(sol.q0())
    print("---------- Q1 ----------")
    print(sol.q1())
    print("---------- Q2 ----------")
    print(sol.q2())
    print("---------- Q3 ----------")
    print(sol.q3())
    print("---------- Q4 ----------")
    print(sol.q4())
    sol.transaction.close()
    sol.session.close()
    sol.driver.close()
