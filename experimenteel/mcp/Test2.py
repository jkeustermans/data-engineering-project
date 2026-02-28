from langchain_ollama import ChatOllama
from langchain_community.utilities import SQLDatabase
from langchain_classic.chains.sql_database.query import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
import re
from langchain_core.runnables import RunnableLambda

DB_URL = "mysql+mysqlconnector://root:password@localhost:3306/test"

llm = ChatOllama(
    model="llama3.2",
    temperature=0,
    base_url="http://localhost:11434"
)

db = SQLDatabase.from_uri(DB_URL)


def extract_sql(text: str) -> str:
    # verwijder eventuele markdown
    text = re.sub(r"```sql|```", "", text, flags=re.IGNORECASE)

    # zoek eerste SELECT statement
    match = re.search(r"SELECT.*?;", text, re.IGNORECASE | re.DOTALL)

    if not match:
        raise ValueError(f"Geen geldige SQL gevonden in:\n{text}")

    return match.group(0).strip()

clean_sql = RunnableLambda(extract_sql)

# Stap 1: maak SQL query generator
write_query = create_sql_query_chain(llm, db)

# Stap 2: tool om query uit te voeren
execute_query = QuerySQLDataBaseTool(db=db)

# Stap 3: combineer correct
safe_chain = write_query | clean_sql | execute_query

# Test
# response = safe_chain.invoke({"question": "How many records are there in table test?"})
response = safe_chain.invoke({
    "question": "geef alle records uit de tabel test",      # soms lijkt dit te werken. complexere queries niet
    "table_info": db.get_table_info(["test"])
    })
print(response)

