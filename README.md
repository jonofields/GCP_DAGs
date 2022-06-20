# GCP_DAGs
<h2>
This is a personal project in order to better familiarize myself with GCP and airflow.  The intended goal is to transfer over data from 
the city of Chicago's data portal into BigQuery for insights and analysis.  Ultimately answering the question, does police funding have a positive
correlation to arrests? (while taking the crime rate into account)
</h2>

<h3>
  Completed so far:
</h3>
<p>Create functioning DAG</p>
<p>Create library folder within DAG to hold functions/scripts</p>
<p>Import library into DAG</p>
<p>Set DAG to run once a week</p>
<p>Download Chicago data in csv format by using curl, name files based on type: so far crime and arrests</p>
<p>Create two functions within lib to upload data to a bucket - 
  tried looping through data directory within one function - settled on individual functions</p>
<p>DAG runs succesfully, data uploaded</p>
<p>Create bash commands to load data into BigQuery</p>

<h3>To Do:</h3>
<p> Redo all completed with funding, yearly budget reports for policing (just add to DAG and lib)</p>
<p>SQL queries to calculate crime rate, avg sal, yearly funding, insights on types of crime, insights on types of arrests</p>
<p>Experiment with data vis to create a map of crime.  Look into mapping long/lat.</p>
