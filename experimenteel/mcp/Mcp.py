from mcp.server.fastmcp import FastMCP
import sqlalchemy

mcp = FastMCP("MySQL_Manager")

DB_URL = "mysql+mysqlconnector://root:password@localhost:3306/test"
engine = sqlalchemy.create_engine(DB_URL)

@mcp.tool()
def execute_sql(query: str) -> str:
    """
    Use this tool to execute a query in the database and return the result to the client.
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(sqlalchemy.text(query))
            rows = result.fetchall()
            # Zet resultaten om naar een leesbare string voor de LLM
            return str([dict(row._mapping) for row in rows])
    except Exception as e:
        return f"Database Error: {str(e)}"

if __name__ == "__main__":
    # Manuele testcode voor connectiviteit te testen
    # with engine.connect() as connection:
    #     result = connection.execute(sqlalchemy.text("select * from test"))
    #     rows = result.fetchall()
    # print(rows)
    mcp.run()