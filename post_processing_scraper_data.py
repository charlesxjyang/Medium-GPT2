#convert list of panda pkl to text file
def convertPklToTxt(paths:list):
    texts,titles = [],[]
    for path in paths:
        df = pd.read_pickle(path)
        #clean up
        #df['publication'] = 
        #df['title'] = 
        all_string = df['title'].copy(deep=True)
        df['title'] = [x[0] for x in all_string.str.split(' - ')]
        df['publication'] = [x[1] if len(x)>1 else None for x in all_string.str.split(' - ')]
        df['text'] = df['text'].str.replace('\nWritten by\n',"\n")
        texts.append(df['text'])
        titles.append(df['title'])
    pd.concat(texts).to_csv('data/all_texts.txt',header=False,index=False)
    pd.concat(titles).to_csv('data/all_titles.txt',header=False,index=False)
    
def get_total_number_of_articles(paths:list):
    dfs = []
    for path in paths:
        df = pd.read_pickle(path)
        dfs.append(df)
    df = pd.concat(dfs)
    print(len(df))