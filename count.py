import json
from tqdm import tqdm
count = {}
def sortedDictValues1(adict):

    items = adict.items()

    items = sorted(items)

    return {key:value for key, value in items}, sum([v for k, v in items])

if __name__ == '__main__':
    f = open("problem_corpus\p5.txt", "r", encoding='utf-8')
    for line in tqdm(f):
        data = json.loads(line)
        if data['trial_date']:
            year = data['trial_date'][:4]
            if not year in count:
                count[year] = 0
            count[year] += 1
    print(sortedDictValues1(count))
