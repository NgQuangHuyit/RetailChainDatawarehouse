from datetime import timedelta
from datetime import date
import random
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



if __name__ == "__main__":
    print(randomDateFromTo(date(1950, 1, 1), date(2010, 12, 31)))