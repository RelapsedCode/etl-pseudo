# Project overview
Pseudo-etl program which with the following main functions:  
* generate_random_json - returns a single json containing random (PRNG) key, value, and the current timestamp.  
* read_from_file - returns list of json strings
* insert_into_db - inserts a single json object into a postgre database  

Fluent user interface is achieved using two functions - source and sink which return the self instance.  
Run function verifies the string arguments with which source and sink are called and respectively calls the other 3 main functions or / and prints on the console.  

# Configuration
You can call the functions with the following line:  
ETL().source("Simulation").sink("Console").run()

The arguments for source() can be:
* "Simulation" - calls _generate_random_json_ and passes the object to the output till the execution is interrupted  
* "file_path" - input the file path to your json will call _read_from_file_ function
read_from_file
  
The arguments for sink() can be:  
* "PostgreSQL" - saves the object to a postgre database
* "Console" - print the results on the console  

Examples:  
\#ETL().source("Simulation").sink("Console").run()  
\#ETL().source("json\data_file_generated.txt").sink("PostgreSQL").run()  

File description:  
There are no dependencies between the files.  
_main.py_ - the main program  
_json_file_generator.py_ - creates a txt file with random json strings inside  
_procedural_code.py_ - blueprint for the main program  