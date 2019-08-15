METHODOLOGY / APPROACH TO SOLVING THE CHALLENGE IN THE PROJECT

Untitled.ipynb contains my initial approach to solving the problem locally before proceeding to AWS

The first challenge was that the two sample data were zipped so I had to import zipfile library to unzip log-data.zip and song-data.zip

The output of the unzip is the log-data and song-data folders in the data folder

Now that I have the data unzipped, spark.read.json did the trick of getting the data unto the data frame 

Having got the dataframe it was pretty stright forward to get the needed columns with select attribute of ther dataframe

the sort attrubute was used for partition where needed, dropDplicate was used to remove repititive data

Then .write.save("data/schema/artist_table", format="json", header=True) was used to write the parquet files back to drive

All written data are currently stored in the schema folder

After success locally it was now time to go try it out on AWS

I created the EMR with spark and created a notebook from the cluster

The notebook is the sparkify.ipynb that I later downloaded after I was done processing

Since I had a notebook connected to my Spark EMR cluster I simply had to repeat all what I had done locally

I had a little twick here and there and things took a bit longer to run because of more data

I ensured all the work I did were all in the same region of wes-oregon

I was able to write back to my own S3 bucket 's3a://olayemiodefunsho/

After which I downloaded the notebook and moved the code to the etl.py file


RUNNING THE SCRIPT FILE
Create an EMR cluster with a private key, with the PEM file downloaded to my local machine

Create a rule in the security group to allow inbound traffic from port 22

Download the etl.py file to my local machine download folder

use the scp command to move the etl.py to the EMR cluster using : scp -i Downloads/spark_cluster.pem Downloads/etl.py hadoop@ec2-34-222-156-51.us-west-2.compute.amazonaws.com:

SSH to the cluster from my local machine with the PEM file created using : ssh -i Downloads/spark_cluster.pem hadoop@ec2-34-222-156-51.us-west-2.compute.amazonaws.com

Now that our script is on the EMR cluster, the script can be run using : /usr/bin/spark-submit --master yarn etl.py


DESCRIPTION OF FILES IN THE PROJECT
data folder : contains input and output data used while solving the challenge locally

sparkify.ipynb : I downloaded this from the Notebook I used for the EMR cluster on AWS to also test after I was done locally

Untitled.ipynb : The notebook I used to test locally

etl.py : This the script that has the final solution to the project. This has to be uploaded to a live EMR cluster to solve the project challenge

README.md : This is me!




