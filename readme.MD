This project allows download and sync of the vicmap suite of data products to a local PostGis database.

Author: Warwick Wilson
Company: Department of Transport and Planning (Victoria)
Date: 13/11/23
Revised: 30/8/24


Step 1. SETUP

A. Install Postgres v16 or above from the EDB website and install the spatial extension. 
   (You can manually install PostGis as the superuser: "CREATE EXTENSION PostGIS;" if needs be.)
   
	Create an instance (vicmap?) and a user (vicmap?) with permission to create schemas.
   eg: log in as the database master and use: 
	   CREATE ROLE vicmap with PASSWORD 'vicmap' WITH LOGIN;
      GRANT CREATE ON DATABASE vicmap TO vicmap;

	NB: EDB installer uses stack builder to install the new spatial db for you.
   NB2: Minor cleanup -- You can delete the tiger/tiger_data/topoology schemas.

	Install a database client such as DBeaver to allow access to the database instance for maintenance.
	 
B. Install python version 3.10 or better, pip install the libraries for psycopg2 and requests.

C. Populate the config.ini file with your connection details and your email address.
   NB: If you are using multiple instances please use the plus extension to your base address.
	     ie: person@place.com and person+prod@place.com

D. execute the python script with the setup argument
	 $> deltaVic.py setup
	 It will intiate a client Id for your email address and place it in your config.ini file

E. Go to your email and confirm the account so it can be acrtivated.

F. run setup a second time.
	 $> deltaVic.py setup
	 It will place the default public api-key in the config.ini file.
	 It will then create the vicmap schemas and populate the table deltavic.layer_registry (511 public layers)

G. If you do not want all datasets delivered, go to the table created above set active to false.
   OR, "update vm_delta.layer_registry set active=false where identity like 'vlat%'", then set the ones you want to true.

Step 2. POPULATE

A. Manually run the comamnd 
	 $> deltaVic.py sync
	 It will populate your active layers with downloads from the Vicmap Manager application.

Step 3. AUTOMATE

A. Queue a cron job or windows scheduler to run every hour with the command:
	 $> deltaVic.py sync
	 Please set it for a randome minute duing the hour to spread the load on our servers.
	 Ensure that the process is not already running when instantiating.
