scrapes all coin data from a popular coin tracker

designed to be run on a schedule and pull data into a data warehouse

the data warehouse stores the data in it's raw format giving you the ability to post-process the complex data

there is a script to post-process the data, i use the data warehouse API to get my data and build a pandas dataframe that i then pass into this post-processing class
