from aslite.db import get_papers_db, get_metas_db, get_tags_db
import argparse
import pandas as pd
import time

# Example usage: python zotero_ingest.py -f zotero_ingest.py -u dveni

if __name__ == "__main__":

    timenow = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                            
    parser = argparse.ArgumentParser(description='Zotero Ingest')
    parser.add_argument('-f', '--file', type=str, help='csv file to ingest')
    parser.add_argument('-u', '--user', type=str, help='user to ingest')
    parser.add_argument('-t', '--tag', default=f'zotero_{timenow}', type=str, help='tag to ingest')
    args = parser.parse_args()


    pdb = get_papers_db(flag='c')
    mdb = get_metas_db(flag='c')


    def add_tag(pid, tag):
        with get_tags_db(flag='c') as tags_db:

            # create the user if we don't know about them yet with an empty library
            if not args.user in tags_db:
                tags_db[args.user] = {}

            # fetch the user library object
            d = tags_db[args.user]

            # add the paper to the tag
            if args.tag not in d:
                d[args.tag] = set()
            d[tag].add(pid)

            # write back to database
            tags_db[args.user] = d

    def store(p):
        pdb[p['_id']] = p
        mdb[p['_id']] = {'_time': p['_time']}
        add_tag(p['_id'], args.tag)

    def is_valid(p):
        valid = True
        if not ('DOI' in p and 'Title' in p and 'Abstract Note' in p and 'Author' in p):
            valid = False
        if type(p['DOI']) is not str:
            valid = False
        if type(p['Title']) is not str:
            valid = False
        if type(p['Abstract Note']) is not str:
            valid = False
        if type(p['Author']) is not str:
            valid = False
        
        # if type(p['Automatic Tags']) is not str:
        #     valid = False
            
        return valid
    invalid_papers = []
    df = pd.read_csv(args.file)
    for i, row in df.iterrows():
        p = row.to_dict()

        if not is_valid(p):
            print(f"skipping invalid paper {p['Key']}")
            invalid_papers.append(p)
            continue
        
        p['_id'] = p['DOI'].replace('/', '-')
        p['_time'] = time.time()#time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        p['_time_str'] = time.strftime("%b %d %Y", time.gmtime())
        p['title'] = p['Title']
        p['summary'] = p['Abstract Note']
        p['tags'] = [{'term': 'cs.CV',
                        'scheme': 'http://arxiv.org/schemas/atom',
                        'label': None}]#p['Automatic Tags'].split('-') #p['Manual Tags'] 
        p['authors'] = [{'name': name for name in p['Author'].split(';').reverse()}]
        

        pid = p['_id']

        if pid not in pdb:
            store(p)
            print(f"stored {pid}")

    print(f"done, {len(invalid_papers)} papers were invalid")