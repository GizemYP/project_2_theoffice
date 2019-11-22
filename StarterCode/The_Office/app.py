import os
import psycopg2
import pandas as pd
import matplotlib
from matplotlib import style
style.use('seaborn')
import matplotlib.pyplot as plt
import csv

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

from flask import Flask, jsonify, render_template
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)


#################################################
# Database Setup
#################################################

app.config["SQLALCHEMY_DATABASE_URI"] = 'postgresql+psycopg2://koxrqvuuxugccs:6878ba0790ec132289e57f3ed6daa22ada7e6368bea3a7489f4c03cb2bfb9ff1@ec2-174-129-214-42.compute-1.amazonaws.com/d64jo158fls6ag'
db = SQLAlchemy(app)

@app.route("/")
def index():
    """Return the homepage."""
    return render_template("index.html")


@app.route("/episodes")
def names():
    """Return a list of sample names."""

    # Use Pandas to perform the sql query
    main_df_i = pd.read_sql('''
	select
	l.s0e0,
	speaker,
	line_text,
	p.ep_id,
	title,
	synopsis,
	imdb_rating
	from lines l
	join prim_key p
		on p.s0e0 = l.s0e0
	join metadata m
		on p.ep_id = m.episode_no
	''', db)

    episodes = main_df_i["s0e0"].unique()

    # Return a list of the column names (sample names)
    return jsonify(episodes[0])

@app.route("/samples")
def samples(sample):
    """Return `otu_ids`, `otu_labels`,and `sample_values`."""

	#main_df_top_cast = main_df_i.groupby(['speaker']).sum().sort_values(by="line_text", ascending=False).reset_index()
	#cast_df = main_df_top_cast.loc[main_df_top_cast['line_text'] > 150]
	#top_cast = cast_df['speaker'].values
	#main_df_f = main_df_i[main_df_i['speaker'].isin(top_cast)]

    stmt = db.session.query(Samples).statement
    df = pd.read_sql_query(stmt, db.session.bind)

    pie_df_i = pd.melt(df, id_vars =['otu_id','otu_label'],var_name='sample')\
	.groupby(['otu_id','otu_label']).sum().reset_index().sort_values(by=['value'],ascending=False).head(10)
	
    pie_df_i['perc']= round(pie_df_i['value']/pie_df_i['value'].sum()*100,2)
    # Format the data to send as json
    data = {
        "otu_ids": pie_df_i['otu_id'].tolist(),
        "sample_values": pie_df_i['perc'].tolist(),
        "otu_labels": pie_df_i['otu_label'].tolist(),
    }
    return jsonify(data)


@app.route("/metadata/<sample>")
def sample_metadata(sample):
    """Return the MetaData for a given sample."""
    sel = [
        Samples_Metadata.sample,
        Samples_Metadata.ETHNICITY,
        Samples_Metadata.GENDER,
        Samples_Metadata.AGE,
        Samples_Metadata.LOCATION,
        Samples_Metadata.BBTYPE,
        Samples_Metadata.WFREQ,
    ]

    results = db.session.query(*sel).filter(Samples_Metadata.sample == sample).all()

    # Create a dictionary entry for each row of metadata information
    sample_metadata = {}
    for result in results:
        sample_metadata["sample"] = result[0]
        sample_metadata["ETHNICITY"] = result[1]
        sample_metadata["GENDER"] = result[2]
        sample_metadata["AGE"] = result[3]
        sample_metadata["LOCATION"] = result[4]
        sample_metadata["BBTYPE"] = result[5]
        sample_metadata["WFREQ"] = result[6]

    print(sample_metadata)
    return jsonify(sample_metadata)




if __name__ == "__main__":
    app.run()
