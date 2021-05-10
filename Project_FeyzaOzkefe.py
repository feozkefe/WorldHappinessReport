# Import Libraries
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import col, lit
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.mllib.stat import Statistics
import pandas as pd
from pyspark.mllib.stat import Statistics
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation
import seaborn as sns
import matplotlib.pyplot as plt

if __name__ == "__main__":
    # Prepare spark session with configuration
    spark = SparkSession.builder.master("local[*]").appName('MyData').getOrCreate()

    # Prepare spark context
    sc = spark.sparkContext

    # Load the Datasets
    df_2015 = spark.read.csv('2015.csv', sep=",", header=True)
    df_2016 = spark.read.csv('2016.csv', sep=",", header=True)
    df_2017 = spark.read.csv('2017.csv', sep=",", header=True)
    df_2018 = spark.read.csv('2018.csv', sep=",", header=True)
    df_2019 = spark.read.csv('2019.csv', sep=",", header=True)
    df_2020 = spark.read.csv('2020.csv', sep=",", header=True)

    # Preprocessing Each Data into Same Format Columns

    # Add year column to datasets
    df_2015 = df_2015.withColumn("Year", lit("2015"))
    df_2016 = df_2016.withColumn("Year", lit("2016"))
    df_2017 = df_2017.withColumn("Year", lit("2017"))
    df_2018 = df_2018.withColumn("Year", lit("2018"))
    df_2019 = df_2019.withColumn("Year", lit("2019"))
    df_2020 = df_2020.withColumn("Year", lit("2020"))

    # Change the column names

    # 2017
    df_2017 = df_2017.withColumnRenamed("Happiness.Rank", "Happiness Rank") \
        .withColumnRenamed("Happiness.Score", "Happiness Score") \
        .withColumnRenamed("Economy..GDP.per.Capita.", "Economy (GDP per Capita)") \
        .withColumnRenamed("Health..Life.Expectancy.", "Health (Life Expectancy)") \
        .withColumnRenamed("Trust..Government.Corruption.", "Trust (Government Corruption)") \
        .withColumnRenamed("Dystopia.Residual", "Dystopia Residual")

    # 2018
    df_2018 = df_2018.withColumnRenamed("Overall rank", "Happiness Rank") \
        .withColumnRenamed("Country or region", "Country") \
        .withColumnRenamed("Score", "Happiness Score") \
        .withColumnRenamed("GDP per capita", "Economy (GDP per Capita)") \
        .withColumnRenamed("Social support", "Family") \
        .withColumnRenamed("Healthy life expectancy", "Health (Life Expectancy)") \
        .withColumnRenamed("Freedom to make life choices", "Freedom") \
        .withColumnRenamed("Perceptions of corruption", "Trust (Government Corruption)")

    # 2019
    df_2019 = df_2019.withColumnRenamed("Overall rank", "Happiness Rank") \
        .withColumnRenamed("Country or region", "Country") \
        .withColumnRenamed("Score", "Happiness Score") \
        .withColumnRenamed("GDP per capita", "Economy (GDP per Capita)") \
        .withColumnRenamed("Social support", "Family") \
        .withColumnRenamed("Healthy life expectancy", "Health (Life Expectancy)") \
        .withColumnRenamed("Freedom to make life choices", "Freedom") \
        .withColumnRenamed("Perceptions of corruption", "Trust (Government Corruption)")

    # 2020
    df_2020 = df_2020.withColumnRenamed("Country name", "Country") \
        .withColumnRenamed("Ladder score", "Happiness Score") \
        .withColumnRenamed("Logged GDP per capita", "Economy (GDP per Capita)") \
        .withColumnRenamed("Social support", "Family") \
        .withColumnRenamed("Healthy life expectancy", "Health (Life Expectancy)") \
        .withColumnRenamed("Freedom to make life choices", "Freedom") \
        .withColumnRenamed("Perceptions of corruption", "Trust (Government Corruption)")

    # Add index column starting from 0
    df2020_index = df_2020.select("*").withColumn("index", monotonically_increasing_id())
    # Add "Happiness Rank" Column to the 2020 Data (add 1 to index column)
    df2020_index = df2020_index.withColumn("Happiness Rank", col("index") + 1)
    # Drop the index column
    df2020_index = df2020_index.drop("index")

    # Store Each Data from to 2015 - 2020 into New Data Frame

    # BU KISIM SELECT İÇİNE COLUMNS VERİLEREK YAPILABİLİR Mİ? KESİN YAPILIR AMA NASIL?
    """val cols = Seq('Country', 'Happiness Rank', 'Happiness Score', 'Economy (GDP per Capita)', 'Family',
                   "Health (Life Expectancy)", 'Freedom', 'Trust (Government Corruption)', 'Generosity', 'Year')

    df_2015_filtered2 = df_2015.select(cols: _ *)"""

    df_2015_filtered = df_2015.select('Country', 'Happiness Rank', 'Happiness Score', 'Economy (GDP per Capita)',
                                      'Family',
                                      'Health (Life Expectancy)', 'Freedom', 'Trust (Government Corruption)',
                                      'Generosity',
                                      'Year')

    df_2016_filtered = df_2016.select('Country', 'Happiness Rank', 'Happiness Score', 'Economy (GDP per Capita)',
                                      'Family',
                                      'Health (Life Expectancy)', 'Freedom', 'Trust (Government Corruption)',
                                      'Generosity',
                                      'Year')
    df_2017_filtered = df_2017.select('Country', 'Happiness Rank', 'Happiness Score', 'Economy (GDP per Capita)',
                                      'Family',
                                      'Health (Life Expectancy)', 'Freedom', 'Trust (Government Corruption)',
                                      'Generosity',
                                      'Year')
    df_2018_filtered = df_2018.select('Country', 'Happiness Rank', 'Happiness Score', 'Economy (GDP per Capita)',
                                      'Family',
                                      'Health (Life Expectancy)', 'Freedom', 'Trust (Government Corruption)',
                                      'Generosity',
                                      'Year')
    df_2019_filtered = df_2019.select('Country', 'Happiness Rank', 'Happiness Score', 'Economy (GDP per Capita)',
                                      'Family',
                                      'Health (Life Expectancy)', 'Freedom', 'Trust (Government Corruption)',
                                      'Generosity',
                                      'Year')
    df_2020_filtered = df2020_index.select('Country', 'Happiness Rank', 'Happiness Score', 'Economy (GDP per Capita)',
                                           'Family',
                                           'Health (Life Expectancy)', 'Freedom', 'Trust (Government Corruption)',
                                           'Generosity',
                                           'Year')

    df1 = df_2015_filtered.union(df_2016_filtered)
    df2 = df1.union(df_2017_filtered)
    df3 = df2.union(df_2018_filtered)
    df4 = df3.union(df_2019_filtered)
    df_all = df4.union(df_2020_filtered)

    # Change column data types
    df_all = df_all.withColumn("Happiness Rank", col("Happiness Rank").cast("Integer"))
    df_all = df_all.withColumn("Happiness Score", col("Happiness Score").cast("Float"))
    df_all = df_all.withColumn("Economy (GDP per Capita)", col("Economy (GDP per Capita)").cast("Float"))
    df_all = df_all.withColumn("Family", col("Family").cast("Float"))
    df_all = df_all.withColumn("Health (Life Expectancy)", col("Health (Life Expectancy)").cast("Float"))
    df_all = df_all.withColumn("Freedom", col("Freedom").cast("Float"))
    df_all = df_all.withColumn("Trust (Government Corruption)", col("Trust (Government Corruption)").cast("Float"))
    df_all = df_all.withColumn("Generosity", col("Generosity").cast("Float"))
    df_all = df_all.withColumn("Year", col("Year").cast("Integer"))

    # What are the 10 happiest countries in the world in 2020?
    print("--------The 10 happiest countries in the world in 2020--------\n")
    rank2020 = df_all.select("Country", "Happiness Rank", "Year")
    rank2020.filter(rank2020.Year == 2020).show(10, truncate=False)

    # Happiness scores of the top countries by years
    print("--------Happiness scores of the top countries by years--------\n")
    rank = df_all.select("Country", "Happiness Rank", "Year", "Happiness Score")
    rank.filter(rank["Happiness Rank"] == 1).show()

    # Average happiness scores of the top 10 countries by years
    """print("--------Average happiness scores of the top 10 countries by years--------\n")
    rank10 = rank.filter((rank["Happiness Rank"] == 1) | (rank["Happiness Rank"] == 2) | (rank["Happiness Rank"] == 3)
                         | (rank["Happiness Rank"] == 4) | (rank["Happiness Rank"] == 5) | (rank["Happiness Rank"] == 6)
                         | (rank["Happiness Rank"] == 7) | (rank["Happiness Rank"] == 8) | (rank["Happiness Rank"] == 9)
                         | (rank["Happiness Rank"] == 10))

    # MEAN DEĞERİ İÇİN NOKTADAN SONRA DİGİT LİMİT YAPILMASI LAZIMM
    rank10.groupBy("Country").pivot("Year").mean("Happiness Score").show()"""

    # Summary Statistics for the years 2015-2020 World Happiness Report Results
    print("--------Summary Statistics for the year 2015 World Happiness Report Results--------\n")

    df_2015_filtered.describe("Happiness Score", "Economy (GDP per Capita)", "Family", "Health (Life Expectancy)",
                              "Trust (Government Corruption)", "Generosity").show()

    print("--------Summary Statistics for the year 2016 World Happiness Report Results--------\n")

    df_2016_filtered.describe("Happiness Score", "Economy (GDP per Capita)", "Family", "Health (Life Expectancy)",
                              "Trust (Government Corruption)", "Generosity").show()

    print("--------Summary Statistics for the year 2017 World Happiness Report Results--------\n")

    df_2017_filtered.describe("Happiness Score", "Economy (GDP per Capita)", "Family", "Health (Life Expectancy)",
                              "Trust (Government Corruption)", "Generosity").show()

    print("--------Summary Statistics for the year 2018 World Happiness Report Results--------\n")

    df_2018_filtered.describe("Happiness Score", "Economy (GDP per Capita)", "Family", "Health (Life Expectancy)",
                              "Trust (Government Corruption)", "Generosity").show()

    print("--------Summary Statistics for the year 2019 World Happiness Report Results--------\n")

    df_2019_filtered.describe("Happiness Score", "Economy (GDP per Capita)", "Family", "Health (Life Expectancy)",
                              "Trust (Government Corruption)", "Generosity").show()

    print("--------Summary Statistics for the year 2020 World Happiness Report Results--------\n")

    df_2020_filtered.describe("Happiness Score", "Economy (GDP per Capita)", "Family", "Health (Life Expectancy)",
                              "Trust (Government Corruption)", "Generosity").show()

    # Visualizations for data

    # Change dataframe to pandas dataframe to visualize with seaborn
    df_pandas = df_all.toPandas()
    df_pandas.head()

    # Visualize Correlation between variables
    fig = plt.figure(figsize=(8, 8))
    sns.set(style="white", font_scale=1.2)
    df_corr = df_pandas.dropna()[['Happiness Score', 'Economy (GDP per Capita)', 'Family', 'Health (Life Expectancy)',
                                  'Freedom', 'Trust (Government Corruption)', 'Generosity']].corr()
    sns.heatmap(df_corr, fmt='.2f', annot=True, linewidth=2);

    # Change the 2020 data values

    df2020_index = df2020_index.withColumn("Happiness Score", col("Happiness Score").cast("Float"))
    df2020_index = df2020_index.withColumn("Economy (GDP per Capita)", col("Economy (GDP per Capita)").cast("Float"))
    df2020_index = df2020_index.withColumn("Family", col("Family").cast("Float"))
    df2020_index = df2020_index.withColumn("Health (Life Expectancy)", col("Health (Life Expectancy)").cast("Float"))
    df2020_index = df2020_index.withColumn("Freedom", col("Freedom").cast("Float"))
    df2020_index = df2020_index.withColumn("Trust (Government Corruption)", col("Trust (Government Corruption)").cast("Float"))
    df2020_index = df2020_index.withColumn("Generosity", col("Generosity").cast("Float"))

    # Average Happiness Score based on Regions
    df2020_new = df2020_index
    a = df2020_new.groupBy("Regional indicator").mean("Happiness Score")
    a.show()
    # Visualization
    df2020_pandas = a.toPandas()
    df2020_pandas.sort_values(by="avg(Happiness Score)", inplace=True)
    plt.figure(figsize=(10, 6))
    sns.set(font_scale=1.2)
    ax = sns.barplot(x="avg(Happiness Score)", y="Regional indicator", data=df2020_pandas)
    ax.set_title('Average Happiness Score based on Regions')
    ax.set_xlabel("Happiness Score")
    ax.set_ylabel("Region")

    # Change in the average value of happiness in the world
    df_all_new = df_all.groupBy("Year").mean("Happiness Score")
    b = df_all_new.toPandas()
    b.sort_values(by="Year", inplace=True)
    # Visualization
    plt.figure(figsize=(10, 6))
    plt.plot("Year", "avg(Happiness Score)", data=b, label='avg(Happiness Score)')
    plt.axvline(2019, alpha=0.3, linestyle='--', color='r')
    plt.axvline(2021, alpha=0.3, linestyle='--', color='r')
    plt.axvspan(2019, 2021, alpha=0.2, color='r', label='Coronavirus')
    plt.xticks(list(range(2015, 2021, 1)), fontsize=12)
    plt.title('Average Happiness Score by Years', fontsize=18)
    plt.xlabel('Year', fontsize=14)
    plt.ylabel('Happiness Score', fontsize=14)
    plt.yticks(fontsize=12)
    plt.legend()
    plt.show()

    #  Change in the average value of happiness in the Turkey
    df_turkey = df_all.filter(df_all.Country == "Turkey")
    df_turkey_pandas = df_turkey.toPandas()
    # Visualization
    plt.figure(figsize=(10, 6))
    plt.plot("Year", "Happiness Score", data=df_turkey_pandas, label='Happiness Score')
    plt.axvline(2019, alpha=0.3, linestyle='--', color='r')
    plt.axvline(2021, alpha=0.3, linestyle='--', color='r')
    plt.axvspan(2019, 2021, alpha=0.2, color='r', label='Coronavirus')
    plt.xticks(list(range(2015, 2021, 1)), fontsize=12)
    plt.title('Turkey Happiness Score by Years', fontsize=18)
    plt.xlabel('Year', fontsize=14)
    plt.ylabel('Happiness Score', fontsize=14)
    plt.yticks(fontsize=12)
    plt.legend()
    plt.show()

    # Peeking at the all values for Turkey
    fig, axs = plt.subplots(2, 3, figsize=(15, 8))
    fig.suptitle('Variables for Turkey')
    axs[0, 0].plot("Year", "Happiness Score", data=df_turkey_pandas, label='Happiness Score')
    # axs[0, 0].set_title('Happiness Score')
    axs[0, 0].legend(loc="upper right")
    axs[0, 1].plot("Year", "Economy (GDP per Capita)", data=df_turkey_pandas, label='Economy (GDP per Capita)')
    # axs[0, 1].set_title('Economy (GDP per Capita)')
    axs[0, 1].legend(loc="upper right")
    axs[0, 2].plot("Year", "Freedom", data=df_turkey_pandas, label='Freedom')
    # axs[0, 2].set_title('Freedom')
    axs[0, 2].legend(loc="upper right")
    axs[1, 0].plot("Year", "Trust (Government Corruption)", data=df_turkey_pandas,
                   label='Trust (Government Corruption)')
    # axs[1,0].set_title('Trust (Government Corruption)')
    axs[1, 0].legend(loc="upper right")
    axs[1, 1].plot("Year", "Generosity", data=df_turkey_pandas, label='Generosity')
    # axs[1, 1].set_title('Generosity')
    axs[1, 1].legend(loc="upper right")
    axs[1, 2].plot("Year", "Health (Life Expectancy)", data=df_turkey_pandas, label='Health (Life Expectancy)')
    # axs[1, 2].set_title('Health (Life Expectancy)')
    axs[1, 2].legend(loc="upper right")
    plt.show()

    # Economy rate for regions
    df2020_alldata = df2020_new.toPandas()
    region_lists = list(df2020_alldata['Regional indicator'].unique())
    region_economy_ratio = []
    for each in region_lists:
        region = df2020_alldata[df2020_alldata['Regional indicator'] == each]
        region_economy_rate = sum(region["Economy (GDP per Capita)"]) / len(region)
        region_economy_ratio.append(region_economy_rate)

    data_economy = pd.DataFrame({'region': region_lists, 'region_economy_ratio': region_economy_ratio})
    new_index_economy = (data_economy['region_economy_ratio'].sort_values(ascending=True)).index.values
    sorted_data_economy = data_economy.reindex(new_index_economy)
    print("--------Economy rate for regions--------")
    spark_sorted_data_economy = spark.createDataFrame(sorted_data_economy)
    spark_sorted_data_economy.show()
