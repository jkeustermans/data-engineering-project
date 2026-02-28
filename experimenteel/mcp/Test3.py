from langchain_community.utilities import SQLDatabase
from langchain_ollama import ChatOllama
from langchain_classic.chains.sql_database.query import create_sql_query_chain

# 1. Database verbinding
mysql_uri = "mysql+mysqlconnector://root:password@localhost:3306/test"
db = SQLDatabase.from_uri(mysql_uri)

# 2. Llama 3.2 instellen
llm = ChatOllama(model="llama3.2", temperature=0)

# 3. Maak een simpele Chain (geen Agent)
chain = create_sql_query_chain(llm, db)

# 4. Uitvoeren
# vraag = "Genereer de query voor de vraag 'Geef me alle rijen uit de tabel waar de woonplaats Gent of Sint-Niklaas is. Geef enkel het SQL statement als antwoord'"
vraag = "Genereen een sql statements dat alle records teruggeeft waarvan de gemeente in Oost-Vlaanderen ligt"

try:
    # Stap A: Laat Llama de SQL query maken
    sql_query = chain.invoke({"question": vraag})
    
    # Soms zet Llama er 'sql' of extra tekst bij, dit filtert dat eruit
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
    
    print(f"Gegenereerde SQL: {sql_query}")

    # Stap B: Voer de query uit op je database
    resultaat = db.run(sql_query)                   # eennvoudige query lijkt soms te werken
    
    print("\nResultaat uit de database:")
    print(resultaat)

except Exception as e:
    print(f"Fout: {e}")