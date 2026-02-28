# Data-Engineering Project (2025-2026)
## Inhoudstafel
- Inleidend
- Architecturaal
	- Algemeen
	- Beschrijving architectuur
		- Duiding
		- Architectuur
	- Patronen
- Technisch Ontwerp
	- Staging Area
	- Apache Airflow
- Journal
- Algemene Resources
## Inleidend
Dit is een 'Work In Progress'-document en zal aan veranderingen onderhevig zijn. Bedoeling van dit document is om een high-level overzicht te geven van de technische opzet van het project. Tevens zullen er secties opgenomen worden waarin de progressie van het project beschreven wordt (zie sectie Journal). Dit zal twee aspecten bevatten, enerzijds de activiteiten inzake de geïmplementeerde code & configuratie met wat technische duiding, anderzijds het verloop van de opzet. Het laatste aspect dient inzicht te geven in de aanpak, welke concepten tegengekomen zijn, wat de moeilijkere aspecten waren en tenslotte bronverwijzingen.
Om de tekst niet al te zwaar te maken zullen de meeste zaken als bullet points genoteerd worden.
## Architecturaal
### Algemeen
Deze sectie bevat informatie betreffende de architecturale opzet van het project. Dit is dus een high-level beschrijving. Voor verdere details zijn er andere secties in het document opgenomen.
### Beschrijving architectuur
#### Duiding
Het project is gebaseerd op de werking van een bestaande onderzoeksgroep aan de Universiteit Antwerpen behorende tot de faculteit Geneeskunde en gezondheidswetenschappen. De onderzoeksgroep verzamelt gegevens van ziekenhuizen wereldwijd in het kader van antimicrobiële resistentie. Op basis van deze gegevens kunnen analyses gemaakt worden die dan op hun beurt gebruikt kunnen worden voor Antimicrobial Stewardship. Dit alles met het oog op problematiek inzake antimicrobiële resistentie in te dammen. 
De opzet is voorlopig vrij eenvoudig. Via een webapplicatie kunnen ziekenhuizen wereldwijd data ingeven aan de hand van een opgezet Point-Prevalence-Survey dat een bepaald protocol volgt. Het protocol/PPS bevat gegevens mbt. patiënt -en behandelgegevens in het kader van de bestrijding van antimicrobiële resistentie. Deze gegevens worden in een OLTP RDBMS opgeslagen. De bedoeling is dat deze gegevens getransformeerd gaan worden met twee doeleinden in het vooruitzicht:
- Data Scientists en onderzoekers gegevens aanreiken in een formaat dat makkelijk te gebruiken is de R-code voor statistisch onderzoek
- Data Sharing met externe partners (WHO, andere universiteiten/wetenschappelijke instellingen)
Bedoeling is om een data-architectuur op te zetten waarbij gegevens geëxtraheerd, getransformeerd en opgeladen worden naar een formaat dat voldoet aan de eisen van de bovenstaande partijen.
Belangrijk: de implementatie van het project zal een sterk vereenvoudigde opzet zijn van wat er in realiteit geïmplementeerd gaat worden.
#### Architectuur
De **vereenvoudigde** opzet zal bestaan uit een aantal componenten die via Docker-compose opgezet worden (Single Host):
- FTP Server zal fungeren als een Staging Area.
	- Hier zullen bestanden op komen te staan die:
		- De input gaan vormen voor het ELT proces om deze data te converteren naar de gewenste formaten
		- Output bestanden die het resultaat zijn van bepaalde verwerkingen (Reverse ETL)
	- Men zou dit ** met de nodige relativiteit ** kunnen aanzien als een (very) poor man's Datalake
	- Een alternatief hiervoor zou zijn het gebruik van Apache Iceberg
	- Resources:
		- Leerboek Business Intelligence (Peter ter Braake - 2022)
		- Apache Iceberg Explained: A Complete Guide for Beginners
- Apache Airflow
	- Orchestrator die het ELT proces voor zijn rekening neemt
	- Zal bestanden ophalen van de FTP Server
	- Vervolgens zal dit bestand verder in de flow worden verwerkt:
		- Data Cleaning
		- Data Transformation
		- Data Enrichment
	- Het opgeschoonde bestand zal worden weggeschreven naar disk ((voorlopig) bind mount met de host)
		- Dit zal een Docker mount bind zijn zodat het resultaat bewaard blijft nadat de containers vernietigd worden
		- Opmerking: de Airflow workers zullen access hebben tot dit bestand. Dit wil zeggen dat we in principe zouden geconfronteerd kunnen worden met race conditions. Ik veronderstel dat het Airflow proces zo ingericht kan worden dat er slechts 1 Airflow Worker tegelijk het bestand kan verwerken
	-Subcomponenten Airflow
		- PostgreSQL Database
		- Redis
- PostgreSQL
	- Deze database zal fungeren als een Datawarehouse
	- Normaliter worden er hier technologieën gebruikt als Snowflake of Teradata. Echter de dataset is relatief klein en zodus zou PostgreSQL hiervoor dienen te volstaan
	- Opmerking: ook in de specifieke realiteit van de onderzoeksgroep zal er hiervoor een RDBMS gebruikt worden. Twee redenen: dataset is relatief klein en financiële situatie van de onderzoeksgroep laat niet toe dure investeringen te doen
- MongoDB
	- Bedoeling is ook om documents (aggregated content) op te slaan
	- Opmerking: deze case is gekozen voor educatieve doeleinden. In de specifieke realiteit van het project is er voorlopig geen vraag naar
- Cloud Storage/DB (Optioneel)
	- Mogelijk zal er een data transfer gedaan worden naar een Cloud Database zodat externe partners Reporting/Analytics/AI-tooling kunnen loslaten op de gegevens
	- In eerste instantie gaat de voorkeur van het project uit naar Google Cloud Storage & BigQuery
		- Azure zou een alternatief kunnen vormen
- Vector Database (Optioneel)
	- Zou documenten kunnen bevatten mbt. het protocol en de tooling
	- Zou gekoppeld kunnen worden aan een LLM (lokaal of in de cloud) zodat eindgebruikers van ziekenhuizen en externe partners vragen kunnen stellen
- MCP Servers (optioneel)
	- Koppeling MCP server met datawarehouse
	- TODO: uitzoeken mogelijkheden om bevragingen te doen via API in engelse/nederlandse taal
- AI Agent systeem (optioneel)
	- TO DO: dieper bekijken
Opmerking: in het project draait alles op één host. In realiteit zal dit verdeeld worden over meerdere hosts/clusters.
### Patronen
Voor Datawarehousing zijn er een aantal patronen die steeds terugkomen (Leerboek Business Intelligence (Peter ter Braake - 2022))
- Datawarehouse Architectuur
	- Input bronnen (CRM, OLTP RDBMS,...)
	- Staging Area (input bronnen worden (via ETL) overgezet naar de Staging Area)
		- Dit bevat nog de 'ruwe data'
	- Deze ruwe data wordt vervolgens 'opgeschoond'
	- Vervolgens zal deze via het ETL proces worden opgeladen naar de Datawarehouse
	- Aan het Datawarehouse zullen Data Marts gekoppeld worden
	- Deze Data Marts zullen gebruikt worden door Data science tools, Frontend tools, Dashboards,...
- Kimball vs Inmon vs Data-Vault
	- Kimball
		- Zal gebruik maken van gedenormaliseerde sterschema's
		- Feiten-tabellen
		- Dimensie-tabellen
		- TODO: verder kort beschrijven
	- Inmon
		- Datawarehouse zal genormaliseerde data bevatten
		- TODO: verder kort beschrijven
	- Data-Vault
		- Mix tussen normalisatie en dimensioneel modeleren
		- TODO: verder kort beschrijven

## Technisch Ontwerp
### Staging Area
- FTP Server opzet
	- SQL/JSON-Files/... => Data Lake (Ruwe data, Batch, Streaming -> Bewerkte data) => Data Marts, Data Scientists => Rapportering
	- Onderverdeling FTP (Folder structuur)
		/Ruwe data 				/Bewerkte data
			/Bron1					/Project1
				/Tabel1					/Tabel1
				/Tabel2					/Tabel2
			/Bron2					/Project2
				/Tabel1					/Tabel1
				/Tabel2					/Tabel2
	- Enkel de sectie Bewerkte data wordt opengesteld voor anderen die dan deze data kunnen gebruiken
	- Nota: voorzien we een mechanisme waar iedere file-upload wordt bewaard (met datum) voor debugging doeleinden (historiek)?
- FTP opzet duiding
	- Passieve poorten zijn de poorten voor bestandsoverdracht
	- curl zal een poort openen voor de data-overdracht en opent daarvoor een extra WILLEKEURIGE poort
	- Deze extra poort komt uit de voorgestelde passive port range
	- Poort 21 is de controle poort (voor commando's)
	- Passieve poort: nodig voor data overdracht
	- Docker setup
		- docker volume create ftp_volume
	- Curl voorbeeld instructies voor download bestand
		- curl -u airflow:airflow ftp://127.0.0.1/test.txt -o test.txt
		- curl -v --ftp-pasv -u airflow:airflow ftp://127.0.0.1/test.txt -o test.txt
	- docker compose -f ftpserver.yml up -d
### Apache Airflow
- Airflow zal het ETL proces aansturen dmv. het definiëren van flows via Directe Acyclic Graphs (DAGs)
- Installatie via Docker Compose
	- Installatie procedure: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
	- Maakt account aan
- Als er problemen zijn dan clean up en restart van scratch
	- zie 'Cleaning-up the environment'-sectie in link installatie procedure
- Test opzet via de command line
	- docker compose run airflow-worker airflow info
- Url access GUI van Airflow: http://localhost:8080
- Om eigen image te maken met bijvoorbeeld extra python libraries:
	- Zie section: 'Special case - adding dependencies via requirements.txt file' in install document (zie boven)
- Resources voor gebruik FT Operaties via connectors
	- FTP Turorial: https://www.sparkcodehub.com/airflow/operators/ftp-operator
	- FTP Operator: https://airflow.apache.org/docs/apache-airflow-providers-ftp/stable/operators/index.html
- Postgres
	- Aanpassing in docker om port te exposen zodat deze extern kan benaderd worden
	- Makkelijker tijdens development om te zien wat er opgeslagen wordt inzake Apache Airflow flow configuraties (vb. bij aanmaak FTP connectie)
	- Configuratie van FTP connection staat in database (gecheckt)
- Aanloggen container van een Airflow Worker
	- Opstart: docker exec -it XXXXX bash    (met XXXXX = container id van airflow-worker)
- Connection aanmaken
	- UI
	- CLI: airflow connections add 'ftp_server' --conn-json '{ "conn_type": "ftp", "login": "airflow", "password": "airflow", "host": "localhost", "port": 21, "schema": "" }'
- Python packages
	- Om te zien welke python packages er geïnstalleerd staan:
		- Log in op een docker container van een airflow worker (zie boven)
		- Run commando: pip freeze
- Installatie IDE
	- VS Code
	- Er is reeds een dag directory onder de airflow folder voorzien tijdens de opzet van airflow
	- Ga in de project dir staan en open terminal:
		- python3 -m venv venv
		- source venv/bin/activate
		- pip install "apache-airflow[celery,ftp]==3.1.7"
		- selecteer de interpretere in je Visual Studio (CTRL-SHFT-P -> select interpreter -> selecteer venv)
- Opzetten eenvoudige DAG
	- https://airflow.apache.org/docs/apache-airflow/stable/tutorial/fundamentals.html
- Codering Test FTP Dag
	- Resource: https://www.sparkcodehub.com/airflow/operators/ftp-operator
	- Aanmaak docker networks
		- docker network create landingzone
		- docker network create orchestration
	- Aanpassing docker-compose files
	- Aanpassing ftp connection in airflow zodat deze verwijst naar de docker service naam van de ftpserver
	- Aanpassing code
	- Testen
- Hoe testen te draaien van functions in een DAG
	-https://www.geeksforgeeks.org/python/function-annotations-python/   (zie wrapper)
- Link vanuit Python naar Database
	- https://realpython.com/python-sql-libraries/#postgresql
	- https://www.geeksforgeeks.org/python/postgresql-python-querying-data/
	- psycopg library voor PostgreSQL te benaderen: https://www.psycopg.org/docs/usage.html
	- Opmerking: alternatief zou zijn: SQLAlchemy
- Link export DataFrame naar Database via Pandas	
	- https://pandas.pydata.org/docs/user_guide/io.html#sql-queries
- Integratie MongoDB
	- Basis opzet connectiviteit
		- https://www.mongodb.com/docs/languages/python/pymongo-driver/current/connect/
	- Pandas conversion df to json
		- https://docs.vultr.com/python/third-party/pandas/DataFrame/to_json
	- Basis data manipulatie MongoDB & Python
		- https://www.geeksforgeeks.org/mongodb/mongodb-python-insert-update-data/

## Journal
### Week 16 feb
	- Opzet & Config lightweight ftp server (Docker)
	- Testen FTP server via curl
		- Uitzoeken gebruik curl mbt. ftp transmissies (oa. command vs passieve ports)
	- Opzet Airflow (Docker Compose)
		- Geen problemen tegengekomen met opzet
	- Testen Airflow
		- Web UI
		- Command Line instructies specifiek aan Airflow
	- Connection FTP server aanmaken in Airflow via UI en CLI
	- Aanpassing opzet zodat PostgreSQL port exposed is en accessable is buiten container
		- Educatieve/Development doeleinden, geen PRD instelling
	- Aanmaak gezamenlijke docker networks
	- Integratie VS Code voor coderen DAGs in IDE
	- Opzet eenvoudige Test-DAG (op basis van internet resource)
	- Opzet DAGs voor upload en download van data naar FTP server vanaf Airflow
		- Problemen: configuratie heeft wel wat tijd in beslag genomen omdat bestanden en paden niet gevonden werden
		- Oplossing: aanpassing in docker compose file inzake bind mount
	- Aanmaak folderstructuur FTP server (inbound, outbound)
	- Opzet GitHub repository
		- Inchecken project
	- Opzet basis directory structuur in FTP server en mount bind met host in apache airflow
		- Aanpassing configuratie docker & aanpassing DAG
		- Testen
	- Refactoring: introductie annotations voor PythonOperator
		- Nieuwere syntax is met decorators/annotations in de code
	- Toevoegen test verwerking van gedownloade file
	- Toevoegen extra PostgreSQL database instance die zal fungeren als een Datawarehouse + initialisatie script db voor aanmaak schema & test table
	- Aanmaak custom Dockerbuild file + aanpassing Docker Compose voor opnemen PostgreSQL Python dependency in Apache Airflow
	- Implementeren Python Task voor:
		- Uitlezen van de (via FTP) gedownloade csv file via Pandas framework
		- Introduceren gebruik Pandas: toevoegen van een (eevoudige) data cleaning
		- Wegschrijven gegevens van DataFrame naar DWH database
		- Integreren psycopg library voor PostgreSQL
### Week 23 feb
- Opzet MongoDB
	- Schrijven docker compose file
	- Opzet init script voorbereiding MongoDB
	- Testen opzet
	- Aandachtspunt: MongoDB docker dient deel uit te maken van hetzelfde docker netwerk als Apache Airflow
- Integratie mongodb in Airflow proces
	- Opnemen extra library pymongo in Airflow container (rebuild image)
	- Uitzoeken connectie vanuit Python naar MongoDB en toevoegen document aan MongoDB collection
	- Uitbreiden code DAG voor eenvoudige verwerking csv file
	- Testen opzet lokaal (Python)
	- Testen opzet in Airflow
- Experimenteel
	- Geëxperimenteerd met aantal concepten die eventueel in latere instantie opgenomen kunnen worden:
		- MCP/Lokale/LLM/Langchain
			- Technologieën
				- Lokale LLM installatie (Ollama - meerdere modellen)
					- Ollama (meerdere modellen: 3.1, 3.2 en qwen2.5-coder)
				- MCP Server
					- mcp library
				- Langchain
			- Opzet
				- Test Database opgezet (MySQL via docker compose)
				- Aantal test-scenario's geschreven: MCP scenario & Langchain scenario
				- Zie files onder experimenteel/mcp
			- Opmerkingen
				- De eerste resultaten zijn teleurstellend. Ik dien verder uit te zoeken waarom dit zo is. Eén van de factoren is wellicht mijn gebrek aan kennis omtrent MCP/inzet LLMs/... Een andere mogelijkheid is dat het model dat ik lokaal draai misschien niet krachtig genoeg is
				- De code is afkomstig uit online resources en verbetert adh van AI
				- Op moment van schrijven is de code niet volledig duidelijk voor mij zodus ik dien verder studiewerk hieromtrent uit te voeren

## Algemene Resources
- Leerboek Business Intelligence (Peter ter Braake - 2022)
	- Status: gelezen
- Fundamentals of Data Engineering (Joe Reis, Matt Housley - 2022)
	- Status: grotendeels gelezen
- Data Pipelines with Apache Airflow (Bas P. Harenslak and Julian Rutger de Ruiter - 2021)
	- Status: aan het doornemen
- Apache Iceberg Explained: A Complete Guide for Beginners
	- https://www.datacamp.com/tutorial/apache-iceberg?utm_cid=23552157103&utm_aid=188237542770&utm_campaign=230119_1-ps-other~dsa-tofu~data-eng_2-b2c_3-emea_4-prc_5-na_6-na_7-le_8-pdsh-go_9-nb-e_10-na_11-na&utm_loc=9196930-&utm_mtd=-c&utm_kw=&utm_source=google&utm_medium=paid_search&utm_content=ps-other~emea-en~dsa~tofu~tutorial~data-engineering&gad_source=1&gad_campaignid=23552157103&gclid=CjwKCAiAncvMBhBEEiwA9GU_ftMq3AigSW9lUmyxzDBq4enHnF6yBd7A88gZ1fSQ5VFmxz5HPBJTshoCIYQQAvD_BwE
	- Status: diagonaal doorlopen. To Do: dieper doornemen
- Apache Airflow
	- https://airflow.apache.org/docs/apache-airflow/stable
- Pandas documentation
	- https://pandas.pydata.org/docs/
- Pandas in Action (Boris Paskhaver - 2021)
	- Status: aan het lezen