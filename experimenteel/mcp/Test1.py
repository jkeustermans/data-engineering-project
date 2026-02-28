import asyncio
import ollama
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# Instellingen voor de MCP server
server_params = StdioServerParameters(
    command="python",
    args=["experimenteel/mcp/Mcp.py"]
)

system_prompt = {
    "role": "system",
    "content": """
        You are a function calling AI.

        You MUST respond ONLY with a valid JSON object in this format:

        {
        "tool_calls": [
            {
            "name": "execute_sql",
            "arguments": {
                "query": "SELECT ..."
            }
            }
        ]
        }

        Do not return anything else.
    """
}

async def chat_met_db():
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            
            # Beschikbare tools ophalen van de MCP server
            tools = await session.list_tools()
            
            # Maak de tool definitie klaar voor Ollama
            ollama_tools = [{
                'type': 'function',
                'function': {
                    'name': 'execute_sql',
                    'description': 'Execute a SQL SELECT query on the MySQL database to retrieve data.',
                    'parameters': {
                        'type': 'object',
                        'properties': {
                            'query': {
                                'type': 'string',
                                'description': 'The full SQL query string, e.g., "SELECT * FROM test"',
                            }
                        },
                        'required': ['query'],
                    },
                }
            }]

            # user_input = "voer de volgende query uit: select * from test . En stuur de data terug die het resultaat is van deze query"
            user_input = """
                Execute a query to retrieve all records from the 'test' table. 
                Return all relevant records.
            """

            full_prompt = f"{user_input}\n\nConstraint: Use the provided tool to get the data."

            # 1. Stuur vraag naar Ollama
            response = ollama.chat(
                model='llama3.2',
                messages=[
                    {'role': 'system', 'content': 'You are a data fetcher. Always use tools for database requests. STRICT RULE: You are not allowed to answer with text unless you have first called the execute_sql tool.'},
                    {'role': 'user', 'content': full_prompt}
                ],
                tools=ollama_tools,
                options={'temperature': 0} # Zet temp op 0 voor striktere tool-calling
            )

            # 2. Check of Ollama een tool (SQL query) wil gebruiken
            if response.get('message', {}).get('tool_calls'):
                for tool_call in response['message']['tool_calls']:
                    tool_name = tool_call['function']['name']
                    arguments = tool_call['function']['arguments']
                    
                    print(f"--- Ollama genereert query: {arguments['query']} ---")
                    
                    # 3. Voer de tool uit via de MCP Server
                    result = await session.call_tool(tool_name, arguments)
                    
                    # 4. Geef resultaat terug aan Ollama voor eindrapportage
                    final_response = ollama.chat(
                        model='llama3.2',
                        messages=[
                            {'role': 'user', 'content': user_input},
                            response['message'],
                            {'role': 'tool', 'content': str(result.content), 'name': tool_name}
                        ],
                        options={'temperature': 0}
                    )
                    print(f"Assistent (if): {final_response['message']['content']}")
            else:
                print("--- DEBUG INFO ---")
                print(f"Model antwoord: {response['message']['content']}")
                if 'tool_calls' in response['message']:
                    print("Er zijn tool_calls maar ze zijn niet goed geformatteerd.")
                else:
                    print("Het model heeft totaal geen tool-call gegenereerd.")
                print("------------------")
if __name__ == "__main__":
    asyncio.run(chat_met_db())