# Data-Engineering Project (2025-2026)
## Inhoudstafel
- Inleidend
- Plan van aanpak
- Functionele beschrijving (High-Level)
- Architecturaal
	- Algemeen
	- Beschrijving architectuur
		- Duiding
		- Architectuur
	- Patronen
		- Datawarehouse architectuur
		- Methode van Kimball
		- Orchestration
- Technisch Ontwerp
	- Staging Area
	- Orchestrator
	- Datawarehouse
	- Document Database
	- Google Cloud
- Data Model
	- Landingzone
		- Structuur Datamodel
		- Beschrijving Datamodel
	- Datawarehouse/OLAP Model
		- Structuur model
- Implementation
	- FTP Server
	- Apache Airflow
	- Google BigQuery
	- MogoDB
- Finale Target Architectuur
- Journal
- Reflectie Projectopdracht
- Geraadpleegde bronnen

## Inleidend
Bedoeling van dit document is om een high-level overzicht te geven van de technische opzet van het project op verschillende niveaus. De niveaus die beschreven worden zijn:
- Architectuur
- Technisch Ontwerp
- Data Model
- Implementatie

Er zal tevens een sectie opgenomen worden waarin de progressie van het project beschreven wordt (zie sectie Journal). Dit dient een chronologisch beeld te geven welke activiteiten er hebben plaatsgevonden. 
Het laatste deel van dit document zal nog twee extra zaken bevatten:
- Een (ruwe) architectuur van het systeem zoals het in de praktijk ontwikkeld zou kunnen worden. Immers het project zoals het voor dit opleidingsonderdeel ontwikkeld is geweest, is een vereenvoudigde vorm van een project dat werkelijk geïmplementeerd zal worden
- Reflectie over het project. Dit wil zeggen: hoe heb ik het aangepakt, wat zijn de dingen die ik geleerd heb, welke moeilijkheden ben ik tegengekomen, welke stukken zijn suboptimaal en dienen verbeterd te worden voor de reële implementatie,...

Het document eindigt tenslotte met bronverwijzingen.

Er zullen ook nog extra documenten toegevoegd worden die dieper ingaan op een aantal elementen mbt. de reële implementatie van het project. Ik beschouw dit als aparte analyses, vandaar dat deze in aparte documenten zijn opgenomen. Deze documenten beschrijven oa. een aantal kandidaat data-architecturen voor de reële implementatie van het project. Tevens bevatten deze de afwegingen die er gemaakt dienen te worden en wat de aandachtspunten zijn.

Opmerking: om de tekst niet al te zwaar te maken zullen in dit document de meeste zaken als bullet points genoteerd worden.

## Plan van aanpak
In eerste instantie had ik een 4-tal fases gedefinieerd die ik zou volgen, startende met een eenvoudige Docker setup. Echter tijdens de ontwikkeling van bedrijfsprojecten wordt meestal in het begin van het project een aantal use cases geïdentificeerd die als risicovol worden aanzien. Deze worden dan reeds in de Elaboration Phase ontwikkeld zodat de risico's zo snel mogelijk gemitigeerd kunnen worden (of indien nodig, kan men vooraan het ontwikkelproces de nodige aanpassingen/alternatieven voorzien). Daarom heb ik besloten om een aantal technische integraties behorende tot de minimale opzet naar het begin van het ontwikkelproces te verplaatsen. Het gevolg is dat Apache Airflow naar het begin van het ontwikkelproces is getrokken.
Voor verdere details inzake de aanpak (oa. gebruik van AI) verwijs ik naar de sectie 'Reflectie projctopdracht'.

## Functionele beschrijving (High-Level)
De nadruk in dit project ligt op het opstellen van een ETL (en een Reverse ETL) Proces dat uitgevoerd zal worden door een Orchestrator library. Binnen deze orchestrator zal de workflow gedefinieerd worden (in de vorm van een Directed Acyclic Graph) die bestaat uit een aantal Tasks die onderling afhankelijk zijn. Deze Tasks zijn onderverdeeld in TaskGroups die toelaten om bepaalde Tasks in Parallel uit te voeren. In grote lijnen kan de verwerking als volgt worden omschreven:
- Negen CSV bestanden staan bijwijze van vertrekpunt klaar in de Outbounds map op de FTP Server
- De orchestrator zal dagelijks (of manueel) een DAG opstarten die de verwerking zal uitvoeren
- De eerste Task van de flow zal de CSV bestanden van de FTP Downloaden naar het lokale filesysteem van Airflow zodat Airflow Workers deze bestanden kunnen accessen
- Vervolgens wordt de eerste TaskGroup uitgevoerd waarbij er 2 parallelle processen worden uitgevoerd:
	- Task 1: het uitlezen van de CSV bestanden, deze in een gedenormalizeerd model gieten (Kimball) en persisteren naar een Datawarehouse
	- Task 2: het uitlezen van de CSV bestanden, deze in document-formaat gieten en vervolgens persisteren naar een document database
- Als deze TaskGroup afgerond is dan wordt de volgende TaskGroup opgestart die tevens bestaat uit 3 taken:
	- Task 1: uitlezen Datawarehouse model en dit OLAP model identiek persisteren naar een Google BigQuery database
	- Task 2: Reverse ETL(1): analyze van de datasets, generatie van grafieken in PNG bestanden en het maken van een korte analyse van de gehele dataset in CSV formaat
	- Task 3: Reverse ETL(1): zorg ervoor dat de bestanden van de analyze terug op de FTP server terecht komen zodat deze door de domein experten (data scientists/onderzoekers) kunnen gedownload worden

Schematisch voorgesteld:

![Data Engineering with Google Cloud Platform](documentation/Screenshot-DAG-Airflow.png)

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
Belangrijk: de implementatie van het project zal een vereenvoudigde opzet zijn van wat er in realiteit geïmplementeerd gaat worden. De finale opzet dient nog verder uitgewerkt te worden. Deze zal elementen bevatten zoals Database Replication. Binnen deze project opzet zal dit weggelaten worden en gaan we uit van een Staging Area waar we files opzetten die dan een (partiële) weerspiegeling zal zijn van de gegevens uit de OLTP Database.
#### Architectuur
De **vereenvoudigde** opzet zal bestaan uit een aantal componenten die via Docker-compose opgezet worden (Single Host):
- FTP Server zal fungeren als een Staging Area.
	- Hier zullen bestanden op komen te staan die:
		- De input gaan vormen voor het ELT proces om deze data te converteren naar de gewenste formaten
		- Output bestanden die het resultaat zijn van bepaalde verwerkingen (Reverse ETL)
	- Dit is erg relatief en men zou dit kunnen zien als 'a (very) poor man's Datalake'
	- Een alternatief hiervoor zou het gebruik zijn van Apache Iceberg of Google dataproc op het Google Cloud Platform
	- Nadelen FTP Server 'als Datalake':
		- Weinig ondersteuning voor data governance
		- Geen fine-grained security
		- Geen opslag van meta-data (geen data-lineage...)
		- Geen mogelijkheid om queries uit te voeren om objecten/files op te zoeken
	- FTP Server gebruiken als Datalake kan dan ook al snel leiden tot een 'Data swamp'
- Apache Airflow
	- Orchestrator die het ELT proces voor zijn rekening neemt
	- Zal bestanden ophalen van de FTP Server
	- Vervolgens zullen de bestanden verder in de flow verwerkt worden:
		- Data Cleaning
		- Data Transformation
		- Data Enrichment
	- Het opgeschoonde bestand zal worden weggeschreven naar disk ((voorlopig) een bind mount met de host)
		- Dit zal een Docker mount bind zijn zodat het resultaat bewaard blijft nadat de containers vernietigd worden
		- Opmerking: de Airflow workers zullen access hebben tot dit bestand. Dit wil zeggen dat we in principe zouden geconfronteerd kunnen worden met race conditions. Ik veronderstel dat het Airflow proces zo ingericht kan worden dat er slechts 1 Airflow Worker tegelijk het bestand kan verwerken
	-Subcomponenten Airflow
		- PostgreSQL Database (bevat de meta-data over de Apache Airflow opzet en concrete Flows)
		- Redis
- PostgreSQL
	- Opgelet: dit is niet de database van de Apache Airflow instance
	- Deze database instance zal fungeren als een 'Datawarehouse'
	- Normaliter worden er hier technologieën gebruikt als Snowflake of Teradata. Echter de dataset is relatief klein en zodus zou PostgreSQL hiervoor dienen te volstaan
	- Opmerking: ook in de specifieke realiteit van de onderzoeksgroep zal er hiervoor een RDBMS gebruikt worden. Twee redenen:
		- De dataset is relatief klein
		- De financiële situatie van de onderzoeksgroep laat niet toe al te dure investeringen te doen
- MongoDB
	- Bedoeling is ook om documents (aggregated content) op te slaan
	- Opmerking: deze case is gekozen voor educatieve doeleinden. In de specifieke realiteit van het project is er voorlopig geen vraag naar (alhoewel dit best in de toekomst wel het geval zou kunnen zijn)
- Cloud Storage/DB
	- Er zal een data transfer gedaan worden naar een Cloud Database zodat externe partners Reporting/Analytics/AI-tooling in de Cloud kunnen loslaten op de gegevens
	- In eerste instantie gaat de voorkeur van het project uit naar Google Cloud Storage & BigQuery
		- Azure zou een alternatief kunnen vormen
- De volgende ideeën zijn wegens tijdsgebrek beperkt gebleven tot de experimentele fase:
	- MCP Servers (optioneel)
		- Koppeling MCP server met datawarehouse
		- Dit is wegens tijdsgebrek beperkt gebleven tot de experimentele fase (AI code generated)
	- Vector Database (Optioneel)
		- Zou documenten kunnen bevatten mbt. het protocol en de tooling
		- Zou gekoppeld kunnen worden aan een LLM (lokaal of in de cloud) zodat eindgebruikers van ziekenhuizen en externe partners vragen kunnen stellen
		- RAG & LLM (Retrieval-Augmented Generation)
		- Ook dit is beperkt gebleven tot de experimentele fase
	- AI Agent systeem (optioneel)
		- Er is hier enkel literatuur omtrent doorgenomen (zie sectie 'Geraadpleegde bronnen')
Opmerking: in het project draait alles op één host. In realiteit zal dit uiteraard verdeeld worden over meerdere hosts/clusters.
### Patronen
Er zijn een aantal architecturale patronen die steeds terugkomen.
#### Datawarehouse Architectuur
- Input bronnen (CRM, OLTP RDBMS,...)
- Staging Area (input bronnen worden (via ETL) overgezet naar de Staging Area)
	- Dit bevat nog de 'ruwe data'
	- Dit kan een Data Lake zijn
- Deze ruwe data wordt vervolgens 'opgeschoond'
- Vervolgens zal deze via het ETL proces worden opgeladen naar het Datawarehouse
- Aan het Datawarehouse zullen Data Marts gekoppeld worden
	- Data Marts is een deelverzameling van een Datawarehouse en is gericht op één specifieke afdeling of business domain
	- Deze Data Marts zullen gebruikt worden door Data science tools, Frontend tools, Dashboards,...

**Schematisch**

![Data Engineering with Google Cloud Platform](documentation/Screenshot-Architectuur.png)

(uit: Data Engineering with Google Cloud Platform - Adi Wijaya - 2024)

- Er zijn verschillende manieren om data te modelleren in Datawarehouses.
- Kimball vs Inmon vs Data-Vault
	- Kimball
		- Zal gebruik maken van gedenormaliseerde sterschema's (of snowflake schema's)
		- Feiten-tabellen
		- Dimensie-tabellen
		- Er zijn verschillende soorten dimensie-tabellen (Degenerated Dimensions, Junk Dimensions, Slowly Changing Dimensions,...)
	- Inmon
		- Datawarehouse zal genormaliseerde data bevatten
		- Access zal niet rechtstreeks op het Datawarehouse gebeuren maar op de Data Marts die gebouwd worden op de Datawarehouse
			- Data Mart is een subset van de Datawarehouse specifiek aan één doel of afdeling binnen het bedrijf
	- Data-Vault
		- Mix tussen normalisatie en dimensioneel modeleren
		- Bevat verzameling gekoppelde tabellen die historische informatie bevat van processen
		- Men maakt onderscheid tussen Hubs, Links en Satellites
			- Hubs: Bevat alleen keys, geen beschrijvende data
			- Links: vergelijkbaar met feitentabellen. Ze beschrijven de relaties die verschillende Hubs onderling hebben. Ze bevatten niet de feiten zelf
			- Satellites: bevatten de dimensie-attributen en de feiten. Gekoppeld via Foreign Keys aan Links / Hubs
#### Methode van Kimball
- Datawarehouse bestaat uit stermodellen die de organisatie beschrijven
- Stermodel heeft 2 soorten tabellen
	- Dimensie-tabel: bevatten de context van de feiten
	- Feiten-tabel: bevat de informatie die gemeten kan worden
- Andere modellering dan deze die gebruikt wordt voor OLTP (genormaliseerd)
- Kimball zal denormaliseren: staat dichter tegen de eindgebruikers en is makkelijker ondervraagbaar via BI tools
- Tijdens het modelleerproces dient het grain niveau bepaald te worden = niveau van detail
	- Als je een kleine grain neemt dan heb je enorm veel rijen met veel detail (kan veel resources vergen voor ondervraging)
	- Als je een grote grain neemt dan zal je minder rijen hebben maar je kan ook minder informatie afleiden omdat je bepaalde informatie geaggregeerd hebt (vb. verkoop van verschillende items tot 1 order bedrag)
- Dimensies
	- Dimensie-tabel bevat een aantal attributen die een dimensie beschrijven (vb. gender van een patiënt)
	- Het zijn platgeslagen tabellen
	- Er wordt vaak gewerkt met datumdimensies (makkelijker om periodes te filteren)
		- Kan extra zaken bevatten zoals het kwartaalnummer, weeknummer, boekjaar (in geval van gebroken boekjaren),...
	- Meestal wordt het aantal dimensies per stermodel beperkt tot 7
	- Er zijn een aantal soorten dimensies
		- Slowly Changing Dimensions
			- Attributen van een dimensie kunnen in de loop der tijd veranderen
			- SCDs zijn een manier om hiermee om te gaan
			- SCD Type 1
				- Oude waarde wordt gewoonweg overschreven, er wordt geen historiek bijgehouden
			- SCD Type 2
				- Bij elke verandering van een attribuut wordt een nieuw record aangemaakt en het oude afgesloten (flag, ingangsdatum en einddatum)
			- SCD Type 3
				- Je houdt meerdere verschillende kolommen bij per attribute (vb. kolom voor vorige waarde en kolom voor huidige waarde)
				- Makkelijker om te vergelijken (query blijft simpel)
		- Conformed Dimensions
			- Binnen een organisatie zullen er meerdere stermodellen zijn
			- Het is good practice om een dimensie-matrix op te stellen om per proces te definiëren welke dimensies relevant zijn
			- Sommige dimensies zal men over meerdere stermodellen willen gebruiken
			- Deze dimensies kunnen generiek gemaakt worden zodat deze dan ook effectief over meerdere stermodellen gebruikt kunnen worden
		- Junk Dimension
			- Bevat elke combinatie van meerdere dimensies en is herleid tot 1 dimensie
			- Je kan bepaalde combinaties van kolommen in je facts table uitsplitsen naar een Junk dimensie zodat je het aantal kolommen beperkt in je Facts table
			- Dit zijn kolommen met lage cardinaliteit (anders zou het aantal combinaties snel explosief toenemen)
			- Vb. combinatie: is-urgent, is-escalated, within-sla voor een bepaalde ticket
		- Degenerate Dimension
			- Een attribute dat geen aparte dimension zal vormen maar dat in de facts table blijft (vb. gewicht van een patiënt in ons model)
	- Er zijn nog veel andere soorten dimensions geïdentificeerd door Kimball (zie boek Datawarehouse Toolkit voor meer details) 
- Feiten
	- Feiten zijn vaak numeriek en/of aggregeerbaar
	- Feiten zijn een meetwaarden die meestal op de finale dashboards terecht komen (vb. KPI's)
	- Feitentabel bevat tevens de foreign keys naar de dimensie tabellen
	- Soorten feiten (belangrijk voor het gebruik ervan op dashboards)
		- Additieve feiten: feiten die over alle dimensies op te tellen zijn (vb. absolute (verkoop)cijfers)
		- Niet-Additieve feiten: kunnen niet zomaar opgeteld worden (vb. percentages)
		- Semi-additieve feiten: kunnen over sommige dimensies opgeteld worden maar over anderen niet
			- Voorbeeld: voorraad. Bij actuele voorraad kan men van product A zeggen dat er op dag 1 nog 10 stuks waren en op dag 2 nog slechts 8. Echter we mogen niet concluderen dat we 18 stuks op voorraad hebben. Maar stel dat we 2 soorten producten hebben A en B en dat we van elks 5 stuks hebben op een bepaalde datum. We mogen dan wel concluderen dat we in totaal 10 stuks op voorraad hebben. Maw. op dimensie datum zijn ze niet optelbaar maar op de dimensie Product wel.
	- Verder zijn er soorten feiten'tabellen'
		- Meeste feiten zijn gevolg van zaken die geregistreerd worden (vb. scan aan kassa = registratie aankoop)
		- Accumulating snapshot: geeft de huidige status van de feiten weer maar feiten kunnen nog aan verandering onderhevig zijn
			- Voorbeeld: men maakt een offerte voor de verkoop van een auto. Deze offerte wordt al dan niet een aankoop. Het bestaande aankoop-feitenrecord kan aangevuld worden met de effectieve aankoopdatum als de aankoop doorgaat
		- Periodieke snapshot: je slaat gegevens over feitentabellen op met een bepaalde frequentie (snapshot). Er is een datumkey die de stand van zaken weergeeft op een bepaalde specifieke datum
#### Orchestration
- Om het ETL proces uit te voeren wordt er gebruik gemaakt van een Orchestration tool. Deze tool zal pipelines opzetten die een aantal systemen met mekaar in verbinding zal brengen. Uit sommige van deze systemen zal data geëxtraheerd worden om deze vervolgens te cleanen, transformeren, enrichen,... en op te slaan in een ander systeem. Een klassiek voorbeeld is het extraheren van data uit een OLTP RDBMS en deze te transformeren naar een gedenormalizeerde vorm die dan in een OLAP Database opgeladen/gepersisteerd zal worden.

**Schematisch**

![Fundamentals of Data Engineering](documentation/Screenshot-Data-Engineering-Lifecycle.png)

(uit: Fundamentals of Data Engineering - Joe Reis - 2022)

- De tool die we gebruiken is Apache Airflow. Alternatieven zijn Prefect of Dagster.
- Er is geopteerd voor Apache Airflow omdat deze technologie een proven status heeft en omdat deze ook de standaard technologie is achter Google Composer (de orchestration tool in het Google Cloud Platform)
- De flow wordt gedefinieerd in DAGs waarbij een Scheduler de DAGs inlaadt en opstart. De logica gedefinieerd in de DAGs zal dan worden uitgevoerd door Workers
- Verder biedt Apache Airflow een aantal mogelijkheden om de flows te monitoren en grafisch weer te geven

**Schematisch**

![Data Pipelines with Apache Airflow](documentation/Screenshot-Airflow.png)

(uit: Data Pipelines with Apache Airflow - Julian de Ruiter - 2026)

## Technisch Ontwerp
### Staging Area
- FTP Server opzet
	- SQL/JSON-Files/... => Data Lake (Ruwe data, Batch, Streaming -> Bewerkte data) => Data Marts, Data Scientists => Rapportering
	- Onderverdeling FTP (Folder structuur)
	```
		/Inbound 				/Outbound
			/Bron1					/Bron1
				/Databron1a				/Databron1a
				/Databron1b				/Databron2b
			/Bron2					/Bron2
				/Databron2a				/Databron2a
				/Databron2b				/Databron2b
	```
	- De Inbound en Outbound folders zullen onderverdeeld worden in subfolders (conceptuele onderverdeling)
		- Inbound = de data die zal geupload worden naar de FTP server
			- Vb. analyses die gebeurd zijn door bepaalde processen (vb. reverse ETL)
		- Outbound = de data die door andere processen (vb. Airflow) zal gedownload worden
			- Vb. de data files die als input gelden voor de DAG (Airflow verwerking)
- Merk op dat in een bedrijf wellicht een full-blown Datalake zal gebruikt worden ipv. een FTP server (er is wel een beperkte literatuur doorgenomen mbt. mogelijke technologieën - zie sectie 'Geraadpleegde bronnen').

### Orchestrator
- Airflow zal het ETL proces aansturen dmv. het definiëren van flows via Directe Acyclic Graphs (DAGs)
	- Definieert Workflows
	- Definieert afhankelijkheden tussen taken
	- Voert de jobs uit
- Airflow zal er ook voor zorgen dat deze flows gemonitord kunnen worden en dat als er zich bepaalde gebeurtenissen voordoen in deze flows dat er dan bepaalde acties ondernomen worden (vb. een aantal retries in geval van failures)
- Er wordt gebruik gemaakt van een aangepaste Docker container zodat er bepaalde Python libraries opgenomen zijn die in de DAG code gebruikt kunnen worden
- De DAG zal in Python classes/code beschreven worden
	- Er zal gebruik gemaakt worden van Helper classes (vb. voor Data Access)
- De Orchestrator zal op verschillende manieren geconfigureerd kunnen worden
	- Meestal wordt een processing gescheduled als zijnde een verwerking die met een bepaalde frequentie plaatsvindt (vb. elke nacht)
- De Orchestrator zal Python classes aanspreken die dan de effectieve logica van de verwerking bevat. Tevens zal de Orchestrator zelf ook bepaalde access abstraheren (vb. connectie naar FTP Server) zodat bijvoorbeeld Datasources in Airflow zelf gedefinieerd kunnen worden
	- Er is gebruik gemaakt van constructor injection zodat de meeste Python classes technologisch onafhankelijk blijven van Airflow (Design Pattern)
- Concrete Flow:
	- Aanmaak DAG met verschillende Tasks en TaskGroups in Airflow
	- Scheduling: elke dag zal DAG draaien
	- Proces:
		- Download CSV bestanden vanuit de FTP Server
		- Deze worden lokaal opgeslagen
		- Persisteren gebeurt voor elke database via een eigen Python OffloadProcessor class die opgeroepen wordt vanuit de DAG-Offload Python code waar de DAG volledig gedefinieerd wordt
		- Persisteren naar Datawarehouse
			- Uitlezen van deze bestanden via Pandas library naar dataframes
			- Persisteren van de data gebeurt op verschillende manieren (louter om educatieve redenen)
				- Via Pandas link naar Database
				- Via Psycopg adapter
				- Via SQLAlchemy library
		- Persisteren naar MongoDB
			- Uitlezen data uit Datawarehouse
			- Linken datasets via Pandas dataframes
			- Inserts via PyMongo adapter
		- Persisteren naar Google BigQuery
			- Uitlezen data vanuit Datawarehouse naar Pandas dataframes
			- Persisteren gaat via de Google Cloud BigQuery library
			- Persisteren gebeurt via LoadJob
			- Beperkingen op Google Cloud Processing
			- Enkel de eerste 5 elementen van elk dataframe worden weggeschreven
				- Reden: free tier -> trage processing en beperkingen
		- Analyse van datasets (REVERSE ETL)
			- DataAnalyzer Python class
			- Genereren meerdere grafieken
				- ATC3 categorieën (tellingen per ATC3 category) (barchart)
				- Prescription Types (tellingen per type) (barchart)
				- Intended Duration per ATC3 category (boxplot)
				- Patient Gender (categorie) (barchart)
				- Patient Weights (bins) (histogram)
				- Algemene analyse dataset (text content) (csv)
			- Deze bestanden worden vanuit Airflow geupload naar de FTP Server (Reverse ETL)
	- Er zijn TaskGroups die Tasks bevatten die parallel zullen uitgevoerd worden
	
### Datawarehouse
- Er zal een klassieke RDBMS worden gebruikt die dienst zal doen als Datawarehouse
	- PostgreSQL
- Consumers: Data Scientists
- Deze zal data bevatten die gedenormalizeerd is
- Er is binnen deze context geopteerd voor Kimball te gebruiken

### Document Database
- Er zal gebruik gemaakt worden van een Documentstore
	- MongoDB
- Consumers: externe partners
- In het huidige project is er geopteerd om voor elke treatment 1 document te voorzien in de database
- Er zijn verschillende manieren om hiermee om te gaan. Men zou ook kunnen geopteerd hebben om de treatment gerelateerde data op te slaan in aparte documenten en die dan te koppelen via keys.

### Google Cloud (BigQuery)
- Deze database zal als Cloud Database fungeren
- OLAP model in DWH wordt identiek gerepliceerd naar BigQuery
- Consumers: externe partners
- De cloud lijkt een interessante oplossing omdat BigQuery direct geïntegreerd kan worden met allerlei tooling, zowel klassieke reporting tooling als Looker Studio, als Machine Learning Tooling (Vertex AI)
	- Looker Studio = maken van dashboards (grafieken en tabellen). Vergelijkbaar met Power BI, Tableau
	- VertexAI, BigQuery ML = voor Machine Learning doeleinden
- Er wordt slechts een beperkt aantal records naar BigQuery weggeschreven omwille van restricties mbt. de free tier van GCP

## Schematisch
Hieronder staat een schematische weergave van de architectuur zoals deze opgezet is voor dit project.

![Datamodel OLAP](documentation/Current-Architecture.png)

## Data Model
### Landingzone
#### Structuur datamodel
Zoals eerder gesteld zal de landingzone een aantal CSV files bevatten die de data omspannen. Deze data is uit de OLTP database getrokken. De files volgen een bepaalde structuur die niet persé relationeel genormaliseerd is maar die een structuur zouden kunnen bevatten zoals datasets die aangeboden worden op het internet. De structuur is dus eerder bedoeld om in een 'educatief project' te fungeren. Bedoeling is dat deze structuur getransformeerd zal worden (zie eerder).
Schema van het datamodel:

![Datamodel landingzone](documentation/Datamodel-Landingzone.png)

#### Beschrijving datamodel
Hieronder staat een oplijsting van de aanwezige datasets met een high-level verklaring. Merk op dat het slechts om een subset van de werkelijke data gaat.
- Institutions
	- Bevat de hospitalen die deel hebben genomen aan bepaalde surveys. In dit geval gaat het om Outpatient Surveys. In deze tabel wordt een kleine dataset aan gegevens bijgehouden.
	- Duiding Kolommen:
		- Country-Code: in welk land is het instituut/hospitaal gevestigd
		- Sub-Region-Code: in welke subregio in de wereld is het instituut gevestigd
		- Subtype-Code: wat voor soort instituut is het (primair hospitaal, secundair hospitaal,...)
- Surveys
	- Een instituut dat meedoet aan een Point-Prevalence-Survey zal een protocol volgen om op bepaalde tijdstippen (survey-date) de patiënten te monitoren en alle gegevens mbt. deze patiënt en zijn behandeling te registreren. Bedoeling is dat er een strikt protocol gevolgd wordt om de data te verzamelen voor elke patiënt op de betreffende survey-date. Aan een Survey zullen dus onderzoekgegevens gekoppeld worden.
	- Duiding kolommen:
		- Inquiry-Id: per jaar worden er een aantal inquiries uitgeschreven. Dit zijn periodes waarin er surveys kunnen gebeuren
		- Institution-Id: de ID van het institution waarop de survey betrekking heeft
- Unit-Registrations
	- Er gebeuren registraties op bepaalde departementen van een instituut/medical care facility = Units. Patiënten waarvoor men registraties gaat doen zullen binnen een bepaalde Unit behandeld worden. Van deze Unit worden ook een aantal gegevens bijgehouden
	- Duiding kolommen:
		- Survey-ID: met welke survey is deze Unit-Registration gekoppeld
		- Survey-Date: datum waarop de survey gebeurd binnen deze unit
		- Medical Specialty Type: wat is de specialiteit van deze Unit (vb. Neurologie)
		- Nbr-of-doctors: aantal doctors gelieerd aan deze unit
		- Nbr-of-Pharmacists: aantal pharmaceuten gelieerd aan deze unit
- Outpatient-Registrations
	- Tijdens een survey worden gegevens genoteerd van de patiënten die gesurveyeerd worden.
	- Duiding kolommen
		- Unit-registration-ID: aan welke Unit is de patiënt gekoppeld
		- Age-group: wat is de leeftijdscategorie van de patiënt (neonaat, kind, volwassene)
		- Gender: gender van de patiënt
		- Weight: gewicht
		- Birth-Weight: gewicht in geval van een neonaat
		- Symptom-codes: codes voor symptomen dat de patiënt vertoond (1 of meerdere gescheiden van mekaar door een pipe)
- Outpatient-Treatments
	- Er gebeuren 2 soorten registraties:
		- Algemene registraties: er wordt geen bijkomende data genoteerd (enkel een aantal basis patiënt gegevens). Dit soort registratie wordt gedaan als er geen antimicrobial wordt voorgeschreven. Deze patiënt wordt wel geregistreerd oa. omwille van het feit dat we willen weten hoeveel patiënten er in totaal zijn gesurveyeerd tov. het aantal patiënten dat een antimicrobial heeft gekregen
		- Detail registratie: hier worden extra gegevens genoteerd, oa. de treatment gegevens. Deze patiënten hebben wel een antimicrobial voorgeschreven gekregen. Merk op dat er meerdere antimicrobials voorgeschreven kunnen worden. Voor elk antimicrobial dat voorgeschreven wordt zal er een Treatment worden geregistreerd
	- Duiding kolommen:
		- Outpatient-Id: aan welke patiënt is de behandeling gelieerd
		- ACT5-Code: de code die het antimicrobial (antibioticum, antiviraal, antifungal) weerspiegelt (classificatie)
		- Prescription-type: soort voorschrift (opstart van behandeling, een voorschrift voor een lopende behandeling,...)
		- Single Unit Dose: dosering van een unit (vb. 1 pilletje)
		- Dose Unit: eenheid (mg, gram, microliter,...)
		- Daily Doses: aantal dosissen per dag
		- Therapy Intended Duration Known: is de duur van de behandeling gekend
		- Therapy Duration: duur van de behandeling
		- Diagnosis-Code: code van de gestelde diagnose (apart classificatiesysteem specifiek aan het project)
		- Indication-Code: waar heeft de patiênt de infectie opgedaan (thuis, in het ziekenhuis,...)
		- Reason in notes: staat de rede voor het toedienen van het antimicrobial in het dossier van de patiënt?
		- Reference Guideline Exists: is er een richtlijn aanwezig voor het gebruik van het verstrekte antimicrobial?
		- Drug According To Guideline: is het toegediende antimicrobial effectief volgens de richtlijn voor het gebruik ervan?
		- Dose According to Guideline: stemt de dosis die gegeven is overeen met de richtlijn?
		- Duration According to Guideline: stemt de duur van de behandeling overeen met de richtlijn?
		- ROA According to Guideline: is de manier van toedienen (oraal, parenteraal,...) volgens de richtlijn?
- Countries (ondersteuningstabel)
	- Bevat code en naam van landen (institution refereert hiernaar)
- Subregions (ondersteuningstabel)
	- Bevat code en naam van de region (institution refereert hiernaar)

### Datawarehouse/OLAP Model
#### Structuur model
- De data die hier betrokken is (voor meer details zie eerdere beschrijving van het model):
	- Institution = instituut/hospitaal waar de PPS gebeurd is. Aan een institution is ook geografische informatie gelieerd (land en subregion)
	- Survey = survey die gedaan wordt voor een bepaald instituut voor een bepaalde inquiry (duidt een periode aan)
	- Unit-Registration = een afdeling van een instituut waar er een PPS gebeurt. Voor elke survey kunnen er meerdere unit registrations zijn. Merk op dat we van een unit registration de medische specialiteit van die unit bijhouden (vb. neurologie), het aantal dokters gelieerd aan de unit en het aantal pharmaceuten gelieerd aan de unit
	- Patient-Registratie: van de patient houden we oa. bij: de leeftijdscategorie, het gewicht, het geboortegewicht, de gender en 1 of meer symptomen. Een patient is altijd gelieerd aan een unit-registration
	- Treatments: een patient kan een of meerdere treatments krijgen. Van treatments houden we oa. bij: de dosering, het aantal dosissen per dag, het toegediende antimicrobial, de gestelde diagnose van de treatment, de indicatie waar de patient de infectie heeft opgelopen, het antimicrobial dat tijdens de behandeling gebruikt wordt en een aantal kwaliteitsindicatoren zoals de aanwezigheid of afwezigheid van richtlijnen, of de duur van de behandeling volgens de richtlijn was, of de dosering de richtlijn volgde,...
	- Diagnose: classificatiesysteem inzake diagnoses (specifiek aan dit project)
	- Indication: specificeer waar men vermoedt dat de infectie opgelopen is (vb. in het hospitaal)
- Hieronder een schematische voorstelling van het gedenormalizeerde model.

![Datamodel OLAP](documentation/Datamodel-OLAP.png)

- Enkele korte toelichtingen inzake de voorgestelde structuur
	- Dimensie tabellen bevatten eigenlijk de zaken waarop je wil filteren/groeperen en niet mee wil rekenen
		- Zaken waarmee men wil rekenen zitten in Fact tables
	- Institution is 'gedenormaliseerd' opgenomen in Dim_Department om een hiërarchische structuur te vermijden (snowflake structuur)
		- Makkelijker voor de mensen die de BI-tools gebruiken
	- Gewicht van patient staat in Fact table en niet in de Patient-dimension table
		- Gewicht is een continue getal en past dus niet in de combinatorische methode van een dimension tabel
		- Het is dan beter om het als een DEGENERATED dimension op te nemen in de Facts table
		- Door het mee op te nemen in de Fact table kan je sneller rapportages maken als: correlatie tussen gewicht en de dosering van het antimicrobial
	- Geographic als aparte dimensie en niet gekoppeld aan Institution:
		- Makkelijker om queries te doen op geografisch niveau (gebeurt veel).
		- Nadeel: kan inconsistent worden met institution. ETL proces dient consistentie te garanderen
        - Vaak zijn er queries die per land of per regio de treatments gaan opvragen ongeacht het institution (geografische slices)

## Implementatie
### FTP Server
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
- Installatie via Docker Compose
	- Installatie procedure: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
	- Maakt account aan
- Als er problemen zijn dan clean up en restart van scratch
	- zie 'Cleaning-up the environment'-sectie in link installatie procedure
- Test opzet via de command line
	- docker compose run airflow-worker airflow info
- URL access GUI van Airflow: http://localhost:8080
- Om eigen image te maken met bijvoorbeeld extra python libraries:
	- Zie section: 'Special case - adding dependencies via requirements.txt file' in install document
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
	- Opmerking: alternatief: SQLAlchemy
- Link export DataFrame naar Database via Pandas	
	- https://pandas.pydata.org/docs/user_guide/io.html#sql-queries
- Integratie MongoDB
	- Basis opzet connectiviteit
		- https://www.mongodb.com/docs/languages/python/pymongo-driver/current/connect/
	- Pandas conversion df to json
		- https://docs.vultr.com/python/third-party/pandas/DataFrame/to_json
	- Basis data manipulatie MongoDB & Python
		- https://www.geeksforgeeks.org/mongodb/mongodb-python-insert-update-data/
- Toevoegen Python dependencies aan Airflow image
	- Ga naar Airflow dir
	- Breng Airflow down (docker compose down)
	- Voeg pip install instructies toe aan Dockerfile
	- Doe docker build voor nieuwe image te maken (docker build .)
	- Start Airflow (docker compose up --build -d)  (vergeet build flag niet om alle containers te rebuilden)

### Google BigQuery
- Opzet project in Google Cloud workspace
	- Link: https://cloud.google.com/
	- Kies vervolgens 'Console' om naar de console te gaan
- Opzet Service Account voor tools die via API Google Cloud willen aanspreken
	- Service account jkeustermans aangemaakt als BigQuery Admin. Procdure:
		- Log aan in Google Cloud Dashboard
		- Ga naar IAM and services
		- Create Service Account
		- Vul basisvelden in
		- Maak een nieuwe key aan
		- Key verschijnt in de lijst
		- Ga naar de details van nieuwe key
		- Daarna naar Keys tabblad gaan
		- Add key > Create new key
		- Kies JSON
		- Er wordt automatisch een key gedownload naar je lokale schijf
	- Resource: https://blog.dataengineerthings.org/setting-up-a-google-cloud-service-account-with-json-key-for-authentication-d673e10ea8e7
- Dependency opnemen in Airflow voor aanspreken API Google Cloud
	- pip install google-cloud-BigQuery
	- Maak een nieuwe image aan voor Apache Airflow
- Implementatie code in Airflow
	- Resource: https://blog.coupler.io/how-to-crud-bigquery-with-python/
- Testscenario:
	- Omwille van het feit dat je op een free tier zit zijn de mogelijkheden erg beperkt
	- Om een volledig testscenario te runnen vanaf scratch (zonder data, enkel tabellen):
	- Run het DDL_Setup.sql script onder de cloud directory in Google BigQuery
	- Er wordt een drop gedaan van alle tabellen om vervolgens de tabellen opnieuw aan te maken
	- Run de DAG in Apache Airflow
	- In het bestand Queries.txt onder de cloud directory staan voorbeeld queries voor het bekijken van de data
	- Merk op dat er slechts 5 records worden doorgestuurd naar BigQuery (data privacy & limitatie van data transfer op GCP Free-Tier)
### MongoDB
	- Connecteren: docker exec -it <container-id> bash
	- Aanloggen op MongoDB: mongosh mongodb://test:test@mongodb:27017
		- Gebruiker: test, password: test
	- Database gebruiken: use pps
	- Aanmaak collection: db.createCollection("medical_documents")
	- Doorzoeken collection: db.medical_documents.find()
	- Verwijderen alle documenten: db.medical_records.remove({})

## Finale Target Architectuur
### Inleidend
Zoals eerder gesteld is deze projectopdracht een vereenvoudigde versie van een project dat in latere fase binnen onze onderzoeksgroep geïmplementeerd zal worden. Binnen dit kader heb ik dan ook een voorbereidende high-level architectuur uitgetekend. Wellicht zullen er hier en daar nog elementen schuiven maar de bedoeling is hier om een architecturale oefening te maken die een real-world scenario beschrijft.
### Beschrijving
Hieronder volgt een korte/high-level beschrijving van de target architectuur.
Zoals we zien dat de volledige architectuur kan onderverdeeld worden in een aantal segmenten.
- OLTP gedeelte dat bestaat uit:
	- Load Balancer die het verkeer verdeeld over de backend componenten
	- Webcontainers (redundancy voor high availability) die inkomende vragen inzake de data-entry tool afhandelen
	- Node voor Asycnhrone Processing (generatie statische R-rapporten, data-exports, data-imports,...) 
	- Node voor Reporting
	- Database Node
- OLAP gedeelte en data pipeline(s). Hier zien we de volgende nodes:
	- Landingzone met een aantal componenten
		- Database (replica van Master OLTP DB)
		- Filesystem voor klassieke data opslag
		- Object Storage (dit zou bijvoorbeeld een MinIO kunnen worden, dit is echter nog nader te bepalen)
	- Node voor Orchestrator
		- Op deze node zal Apache Airflow komen te staan
		- Hoe de finale opzet van Airflow eruit zal komen te zien zal nog te bekijken zijn (afhankelijk van de verwachte load en beschikbare financiële resources)
			- Distributed met meerdere workers (CeleryExecutor)
			- KubernetesExecutor (Kubernetes architectuur)
			- LocalExecutor (single node)
		- Er zullen meerdere data-pipelines voorzien moeten worden. Er zal minstens één data-pipeline dienen voorzien te worden voor elke externe partner
	- Node voor de OLAP database
		- Wellicht zal er hier een klassiek RDBMS systeem gebruikt worden
		- Redenen:
			- Onze data hoeveelheden zijn niet erg groot
			- Kostenreductie (wegens weinig werkingsmiddelen)
		- Deze OLAP database zal ook vanuit de Reporting Node gebruikt kunnen worden (vereenvoudigen/optimaliseren R-reporting code)
		- Ook zullen de domein experten deze database kunnen gebruiken voor data-science doeleinden
		- De OLAP database zal ook gebruikt worden om datasets aan te maken die voor externe partijen dienen aangemaakt te worden
		- Eens deze aangemaakt zijn dan kunnen deze overgezet worden naar Cloud databases (vb. GCP, Azure, AWS)
		- Door een lokale OLAP database op te vullen alvorens deze over te zetten naar een Cloud database krijgen we twee voordelen: 
			- De data scientists van ons eigen onderzoeksteam kunnenn ook gebruik maken van deze database indien gewenst
			- Het debuggen in geval van problemen is makkelijker. De testbaarheid gaat naar omhoog wat we kunnen de processing lokaal opstarten en onmiddellijk lokaal de data nakijken
			- Door de verbeterde testbaarheid drukken we ook de kosten omdat we enkel data gaan doorsturen naar een Cloud Database indien we zeker zijn dat de verwerking correct is (geen extra kosten tgv. iteratieve testen)
- Cloud gedeelte
	- In de huidige architectuur is geopteerd voor GCP maar dit kan evengoed een Azure of AWS systeem zijn
	- Voor database is er geopteerd voor BigQuery
	- Een Cloud systeem biedt een aantal zaken out-of-the-box, waaronder onder andere:
		- Integratie met AI/ML Tooling
		- Integratie met BI-Tooling (Dashboards)
		- Data Governance aspecten (auditing mechanismes, data-lineage,...)
	- Belangrijk: er dient nog wel verder onderzocht te worden of er limiterende factoren zijn wat betreft het opladen van gegevens op een Cloud systeem.
		- Door de huidige geografische verschuivingen kan het gevoelig liggen om data op een (Aemerikaans) Cloud systeem te zetten
		- De policy van de externe partij kan ook bepaalde restricties opleggen (WHO is gevoelig voor de plaats van data-opslag)
		- Andere compliancy factoren kunnen ook restricties opleggen
	- Belangrijk is dat de data zowel 'in-rest' als 'in-transfer' geëncrypteerd dient te worden
		- Wellicht zal er geopteerd worden voor de selectie van een Post-Quantum-Cryptography oplossing
			- We willen het 'Harvest Now, Decrypt Later' probleem vanaf het begin afhandelen
### Schematisch

![Datamodel OLAP](documentation/Target-Architecture.png)

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
### Week 2 maart
- Uitbreiden en herwerken technische documentatie
- Studie Google BigQuery
	- Diagonaal/Partieel doornemen Data Engineering with Google Cloud Platform
	- Doornemen artikels
	- Uittesten UI Google Cloud
- Doornemen artikels voor meer duiding huidig data-engineering landschap (zie ook sectie Geraadpleegde bronnen)
	- What is a Data Lake? Definition, Architecture, and Use Cases
	- Apache Iceberg Explained: A Complete Guide for Beginners
	- Object Storage as Primary Storage: The MinIO Story
	- MinIO Docker: Setup guide for S3-Compatible Object Storage
- Implementatie basisscenario voor integratie BigQuery in Airflow workflow
	- Doornemen resources ivm. opzet:
		- Artikel opzet IAM account & key export
		- Artikel integratie Python en BigQuery
	- Opzetten Google Services Account (IAM)
	- Export key file + opzet configuratie in Airflow
	- Secure configuratie Google credentials in .env file + .gitignore
	- Aanpassen Docker build file voor opnamen Python dependency google-bigquery
	- Aanmaak nieuwe Dataset & Tabel in BigQuery
	- Schrijven basis code voor insert data in Google BigQuery + testen Python code lokaal
		- Impediment: omzeilen probleem billing account (oplossing: batch oplossing ipv. streaming)
	- Opname code in Airflow Task + testen opzet in Airflow
		- Impediment: probleem locatie key file
	- Uitbreiden tecnische documentatie ivm. Google BigQuery
- Eerste aanzet OLAP model (Kimball methode)
	- Uitdenken OLAP Use Cases voor Global-PPS project
	- Doornemen basis artikels Kimball opzet
	- Uittekenen initiële opzet
	- Iteratief met LLM's verschillende opzetmogelijkheden doornemen
	- Overleg Domein Expert
- Experimenteel
	- Experimenteren met opzet van MindsDB
		- Docker compose file schrijven
		- Doornemen basisdocumentatie
		- Uitzoeken basisopzet
		- Opzet test database structuur
		- Opzet integratie met database
		- Opzet Agent structuur
		- Testen opzet
		- Impediment: 
			- Rate Limits van Gemini LLM
		-Bedoeling: integratie van LLM met Database zodat Domein Gebruikers een database zouden kunnen ondervragen via Engelse taal ipv. SQL Statements
		- Status: in progress
### Week 9 maart
- Doornemen slides lessen Data Engineering
- Studie LangGraph
	- Partieel doornemen Intro to LangGraph videoreeks (Part 1)
		- LangChain Academy: Foundation: Introduction to LangGraph - Python  (zie geraadpleegde bronnen)
- Studie Vector Databases
	- Artikel: What is a Vector Database & How Does it Work? Use Cases + Examples  (zie geraadpleegde bronnen)
- Vibe coding/code generatie
	- Registreren Claude Pro
	- Experimenteren Code Generatie via chat (voorlopig nog niet via Claude Code)
		- Claude.MD files laten genereren op basis van requirements
		- Generatie prototype Chatbot (RAG & Vector database + REST API)
		- Aanpassen code & bugfixing
		- Import van PDF
		- Klein aantal exploratieve testen 
- Initiële data exfiltratie OLTP
	- Schrijven queries voor ophalen data uit het OLTP systeem
	- Filtering en initiële transformatie van de data
	- Anonymisering
- Implementatie testcode voor database manipulaties in DAG
	- Libraries:
		- psycopg
		- sql-alchemy
	- Opmerking: de eerdere inserts gebeurde via gebruik van het Pandas framework
	- Reden: educatief (vetrouwd geraken met basisgebruik van psycopg en sql-alchemy)
- Doornemen resources ivm psycopg en sqlalchemy
	- Zie Psycopg documentation en SQLAlchemy Tutorial With Examples

### Week 16 maart
- Export gegevens uit OLTP systeem
	- Aanvulen aanpassingen queries
	- Overzetten data
	- Aanmaken schema diagram voor model
	- Documenteren
- OLAP structuur
	- OLAP structuur verder modelleren
		- Verder uitwerken tabellen
		- Consulteren AI voor controle uitwerking
		- Aanmaken schema diagram voor model
- Data Processing
	- Implementatie initiële basiscode voor opvullen facts_treatment table
		- Schrijven basis-sql voor aanmaak facts_treatments table
		- Code werkt in een lokale (non-airflow) context (omwille van testbaarheid)
		- Joinen van twee CSV files (Outpatient_Treatments & Outpatient_Registrations)
		- Transformatie elementen binnen Dataframes
		- Partiële opvulling (Foreign Keys naar Dimension tables worden nog niet opgevuld)
		- Testen code
	- Volgende stappen (TO DO):
		- Dimension tables opvullen
		- Aanvullen foreign key waardes in facts_treatment table
		- Toevoegen documentatie voor toelichting gemaakte keuzes inzake OLAP modellering

### Week 23 maart
- Refactoring Offload code
	- Optimalisatie code
	- Aanmaken class structuur en opdelen methods
- Implementeren offload OLAP structuur
	- Offload Dimensie Patient
	- Offload Dimensie Geographic
	- Offload Dimensie Survey
	- Offload Dimensie Department
	- Offload Dimensie Diagnosis
	- Offload Dimensie Indication
	- Foreign keys in Treatment Facts table opvullen
	- Testbaarheid class verhogen (constructor injection van (locatie) files)
	- Uitgebreid Testen
	- Nakijken Data Probleem (oorzaak incorrecte data OLTP systeem - missing link Country & Subregion = data quality issue)
- Aanpassing DDL Script voor opzet tabellen OLAP
- Correctie/Update in document Data Exfiltratie naar Landingzone.txt
- Volgende stappen:
	- Van lokale class naar integratie in Apache Airflow
	- Integreren data-offload met Google BigQuery

### Week 30 maart
- Integreren OLAP Offload naar Datawarehouse
	- Refactoring OLAPOffloadProcessor class
		- Configuratie bestandslocatie dynamisch maken (mogelijkheid tot configureren class voor verschillende processing scenario's (lokale test, Airflow))
			- Constructor injection 
		- Dynamisch maken links database/dwh
		- Regressietest (lokaal scenario)
	- Integreren offload datawarehouse in Apache Airflow
		- Klaarzetten flows in FTP
		- Schrijven processor voor overzet FTP files
		- Integratie OLAPOffloadProcessor Apache Airflow
		- Schrijven Airflow DAG code voor Airflow verwerking
		- Testen proces
	- Integreren offload naar BigQuery in Apache Airflow
		- Refactoring van OLAPOffloadProcessor code
			- Extractie code voor uitlezen CSV files naar CSVReader class
			- Extractie code voor uitlezen & persisteren data van Datawarehouse naar DatawarehouseDAO
		- Schrijven DDL voor BigQuery (schema & tabellen)
		- Implementeren BigQueryOffloadProcessor
			- Bevat code voor persisteren dataframes met data uit Datawarehouse naar Google BigQuery tables
			- Integreren offload naar BigQuery in Apache Airflow DAG
	- Testen volledige flow (FTP-landingzone -> Persisteren Datawarehouse -> Persisteren Google BigQuery)

### Week 6 april
- Implementeren offload processing naar MongoDB
	- Refactoring bestaande classes (rename, extractie constanten,...)
	- Implementeren documentdb_offload_processor
	- Testen initiële implementatie
	- Regressietesten DAG Processing
- Integreren logica offload in MongoDB in Apache Airflow
	- Implementeren
	- Refactoring/Optimalisatie code
	- Testen
- Doornemen literatuur
	- AI Agents and Applications - With LangChain, LangGraph and MCP (boek)
	- Parallel and Sequential Tasks Topology in the Airflow Task Paradigm (artikel)

### Week 13 april
- Implementeren analyse & visualisatie dataset
	- Analyze dataset
	- Visualiseren via Seaborn
	- Generatie outputbestanden (PNG-files voor grafieken, txt voor beschrijving dataset)
- Aanpassing docker image Airflow voor toevoegen libraries visualisatie (matplotlib, seaborn, PyQt5)
- Integreren Analyze dataset in Apache Airflow
	- Toevoegen verwerking in DAG
	- FTP Upload code toevoegen in DAG
- Uitbreiden technische documentatie
- Tekenen Architectuur diagrammen
	- Current Architecture
	- Final Target Architecture
- Reflecteren over opdracht (en documenteren bevindingen)

## Reflectie Projectopdracht
- Manier van werken:
	- Ik heb zo weinig mogelijk AI trachten te gebruiken
		- Reden: in een leerfase denk ik dat je meer zaken opsteekt als je zelf zaken dient uit te zoeken
		- Ik heb wel regelmatig AI in traditionele vorm gebruikt (vragen stellen via browser)
		- Voor het data governance luik heb ik literatuur doorgenomen. Daarna aan AI gevraagd om alle belangrijke punten op te lijsten. Daarna verder aangevuld met eigen kennis en laten evalueren (iteratief proces)
		- Voor het OLAP datamodel heb ik een model opgesteld en daarna laten evalueren door AI (iteratief proces)
		- Voor de MCP code heb ik code via AI laten genereren
- Moeilijke punten:
	- Data Engineering landschap verkennen en weg vinden in uitgebreid landschap:
		- Veel soorten technologieën
		- Diverse architecturen
		- Cloud technologieën zijn ook erg uitgebreid
	- Bijkomende zaken als Data Governance & Data Privacy die hun eigen complexiteit met zich meebrengen gaande van zaken als data lineage tot data inference, adertjes onder het gras als Data Provider inzake reponsabiliteit,...
	- Google BigQuery: workarounds omdat free tier niet alles toe laat (vb. alles dient in een batch uitvoer te gebeuren, bepaalde DDL kan niet,...)
	- Airflow verkennen (Operators & combinatie FTP connectie als resource in Airflow - versie verschil leidde tot moeilijkheid)
	- Modellering: Kimball & Data Vault
		- Data Vault kwam verwarrend over
		- Kimball is erg uitgebreid
		- Opzet uitgetekend en AI als sparring partner gebruikt
	- Integratie van veel technologieën -> vele libraries met elk hun eigen syntax -> lastig om alles te blijven onthouden
	- AI: alles lijkt een moving target, niet enkel technologieën maar ook manier van werken (vb. manier van werken AI vs Traditioneel)
	- Experiment: MCP naar PostgreSQL lukte niet echt, vragen in natuurlijke taal stellen werkte niet echt goed
- Verbeterpunten
	- Security: passwords in docker compose files
	- Security: SFTP
	- Misschien dat er in de data modellering nog wel zaken verbeterd kunnen worden
	- Docker compose files zullen hier en daar wel verder geoptimaliseerd kunnen worden vermoed ik
	- Race conditions als er meerdere workers zijn die bijvoorbeeld files willen accessen op het filesystem
- To Do
	- Verder uitwerken target architectuur (iteratief)
	- Verder verdiepen in Apache Airflow (en alternatieven)
	- Verder verdiepen in Cloud technologieën
	- Data Modellering: Kimball en Data Vault verder bestuderen
	- Bekijken semantische modellen
	- Verder verdiepen in AI en de integratie met Data Engineering (LLMs, Agents, MLOps,...)
	- Verder verdiepen in data privacy en compliancy aspecten (GDPR & AI Act)

## Geraadpleegde bronnen
### Boeken
- Leerboek Business Intelligence (Peter ter Braake - 2022)
	- Status: gelezen
- Fundamentals of Data Engineering (Joe Reis, Matt Housley - 2022)
	- Status: grotendeels gelezen
- Pandas in Action (Boris Paskhaver - 2021)
	- Status: grotendeels gelezen
- Data Pipelines with Apache Airflow (Bas P. Harenslak and Julian Rutger de Ruiter - 2021)
	- Status: aan het doornemen
- Data Engineering with Google Cloud Platform (Adi Wijaya - 2024)
	- Status: aan het doornemen
- AI Agents and Applications - With LangChain, LangGraph and MCP (Robert Infante - 2026)
	- Status: aan het doornemen
- Data Governance: The Definitive Guide (Evren Eryurek - 2021)
	- Status: volledig gelezen
### Internet Resources (hoofdartikels)
- What is a Data Lake? Definition, Architecture, and Use Cases
	- https://www.datacamp.com/blog/what-is-a-data-lake
	- Status: gelezen
- Apache Iceberg Explained: A Complete Guide for Beginners
	- https://www.datacamp.com/tutorial/apache-iceberg?utm_cid=23552157103&utm_aid=188237542770&utm_campaign=230119_1-ps-other~dsa-tofu~data-eng_2-b2c_3-emea_4-prc_5-na_6-na_7-le_8-pdsh-go_9-nb-e_10-na_11-na&utm_loc=9196930-&utm_mtd=-c&utm_kw=&utm_source=google&utm_medium=paid_search&utm_content=ps-other~emea-en~dsa~tofu~tutorial~data-engineering&gad_source=1&gad_campaignid=23552157103&gclid=CjwKCAiAncvMBhBEEiwA9GU_ftMq3AigSW9lUmyxzDBq4enHnF6yBd7A88gZ1fSQ5VFmxz5HPBJTshoCIYQQAvD_BwE
	- Status: gelezen
- MinIO Docker: Setup guide for S3-Compatible Object Storage
	- https://www.datacamp.com/tutorial/minio-docker
	- Status: gelezen
- Object Storage as Primary Storage: The MinIO Story
	- https://dev.to/ashokan/object-storage-as-primary-storage-the-minio-story-3g39
	- Status: gelezen
- What is a Vector Database & How Does it Work? Use Cases + Examples
	- https://www.pinecone.io/learn/vector-database/
	- Status: gelezen
- LangChain Academy: Foundation: Introduction to LangGraph - Python
	- https://academy.langchain.com/courses/intro-to-langgraph
	- Status: partieel doorgenomen (25%)
- Psycopg documentation
	- Basic Module Usage: https://www.psycopg.org/psycopg3/docs/basic/usage.html#
	- Passing Parameters to SQL Queries: https://www.psycopg.org/psycopg3/docs/basic/params.html
	- Template String Queries: https://www.psycopg.org/psycopg3/docs/basic/tstrings.html
	- Transaction Management: https://www.psycopg.org/psycopg3/docs/basic/transactions.html
	- Status: gelezen
- SQLAlchemy Tutorial With Examples
	- https://www.datacamp.com/tutorial/sqlalchemy-tutorial-examples
	- Status: gelezen
- Parallel and Sequential Tasks Topology in the Airflow Task Paradigm
	- https://medium.com/thedeephub/parallel-and-sequential-tasks-topology-in-the-airflow-task-flow-paradigm-0855d619799f
- Apache Airflow
	- https://airflow.apache.org/docs/apache-airflow/stable
- Pandas documentation
	- https://pandas.pydata.org/docs/
- Cursus Data Engineering AP (Cop Christophe - 2026)