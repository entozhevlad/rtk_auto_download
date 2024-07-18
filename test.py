import pandas as pd
from handlers import form_prefix
def TestCaseAllLines():
    file_path = 'DEF-9xx.csv'
    df = pd.read_csv(file_path, delimiter=';', dtype={'От': str, 'До': str})
    arr = []
    sett = set()
    for index, line in df.iterrows():
        prefix = str(line['АВС/ DEF'])
        low = line['От']
        high = line['До']
        capacity = line['Емкость']
        new_prefix = form_prefix(prefix, low, high, capacity)
        arr.extend(new_prefix)
        sett.update(new_prefix)
    assert len(sett) == len(arr)

if __name__ == '__main__':
   TestCaseAllLines()

