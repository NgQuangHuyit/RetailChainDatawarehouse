from datetime import timedelta
from datetime import date
import random
import pandas as pd
import csv

def randomDateFromTo(start: date, end: date):
    """
    return random date between two dates
    """
    if end < start:
        raise ValueError("end date should be greater than start date")
    else:
        delta = end - start
        random_days = random.randint(0, delta.days)
        return start + timedelta(days=random_days)

def read_csv(file_path , skiprows=0):
    """
    read csv file and return pandas dataframe
    """
    df = pd.read_csv(file_path, skiprows=skiprows)
    return map(lambda a:tuple(a.values()), df.to_dict(orient='records'))


if __name__ == "__main__":
    print(randomDateFromTo(date(1950, 1, 1), date(2010, 12, 31)))