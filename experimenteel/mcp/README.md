# Documentatie Experimentele opzet MCP
## Pip dependencies
- ollama
- stdio
- langchain
- langchain_ollama
- langchain_community
- mcp
- sqlalchemy
- mysql-connector-python

## Installatie
- Installeerr ollama model
- Start model op: ollama run llama3.2
- Om te zien welke modellen er beschikbaar zijn: ollama list
- Om te zien welke modellen er draaien/draaiden: ollama ps
- pip install van bovenstaande modules
- docker-compose opstarten die my-sql database opstart op poort 3306
- Maak database test aan met tabel test bestaande uit 3 velden: id, naam, adres
- Vul deze tabel op met voorbeeld data

## DB-scripts
- CREATE DATABASE `test`
- CREATE TABLE `test` (`id` varchar(32) DEFAULT NULL, `naam` varchar(50) DEFAULT NULL, `adres` varchar(50) DEFAULT NULL)
- INSERT INTO test.test (id, naam, adres) VALUES('1', 'xx', 'yy');