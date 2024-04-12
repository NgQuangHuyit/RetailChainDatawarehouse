from pyspark.sql.dataframe import DataFrame

def showDF(df :DataFrame):
    df.show()