from bs4 import BeautifulSoup
import requests
import json
import re
import os
import csv
from sklearn import tree
from sklearn import svm
from sklearn.ensemble import AdaBoostRegressor
import sys
from nltk.tokenize import RegexpTokenizer
import matplotlib.pyplot as plt
import random
from os import system

zips=[]
N_PAGE=10
CACHE= 'cache_websites.json'
UNVSS='unvss.csv'
UNVSS_TRAIN='unvss_train.csv'
API_KEY='7jQ2P3xiElrJ12n7aDWQzMJOvsRGX05yn13vWfPh'
CURRENT=2017
if not os.path.exists('./cache_websites.json'):
    with open(CACHE,'w') as c:
        c.write('{}')



def read_csv():
    if not os.path.exists(UNVSS):
        return None
    else:
        unvss=[]
        with open(UNVSS,'r',encoding='utf-8') as f:
            reader=csv.DictReader(f,delimiter=',',quotechar = '"')
            for line in reader:
                unvs=Unvs(unvs_tag=None)
                unvs.name=line['name']
                unvs.rank=line['rank']
                unvs.address=line['address']
                unvs.thumbnail=line['thumbnail']
                unvs.n_ug=line['n_ug']
                unvs.page_url=line['page_url']
                
                unvs.zip=line['zip']
                unvs.type=line['type']
                unvs.year_founded=line['year_founded']
                unvs.setting=line['setting']
                unvs.endowment=line['endowment']
                
                unvs.completion_rate=line['completion_rate']
                unvs.cost=line['annual_cost']
                unvss.append(unvs)
        return unvss

def output_csv(unvss):
    with open(UNVSS,'w+',encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=['name','rank','address','thumbnail','n_ug','page_url','zip','type','year_founded',
                                        'setting','endowment','completion_rate','annual_cost'],
                              extrasaction='ignore',delimiter=',',quotechar='"')
        writer.writeheader()
        for unvs in unvss:
            line={}
            line['name']=unvs.name
            line['rank']=unvs.rank
            line['address']=unvs.address
            line['thumbnail']=unvs.thumbnail
            line['n_ug']=unvs.n_ug
            line['page_url']=unvs.page_url
            
            line['zip']=unvs.zip
            line['type']=unvs.type
            line['year_founded']=unvs.year_founded
            line['setting']=unvs.setting
            line['endowment']=unvs.endowment
            
            line['completion_rate']=unvs.completion_rate
            line['annual_cost']=unvs.cost
            writer.writerow(line)

def output_train(unvss):
    with open(UNVSS_TRAIN,'w+',encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=['rank','n_ug','type','history',
                                        'setting','completion_rate','annual_cost','endowment_amount'],
                              extrasaction='ignore',delimiter=',',quotechar='"')
        writer.writeheader()
        for unvs in unvss:
            line={}
            line['rank']=int(unvs.rank)
            line['n_ug']=int(unvs.n_ug)
            line['type']=unvs.type
            line['history'],line['endowment_amount']=unvs.numerize_attribute()
            line['setting']=unvs.setting
            line['completion_rate']=float(unvs.completion_rate)
            line['annual_cost']=int(unvs.cost)
            writer.writerow(line)
def get_soup(url):
    headers={'User-Agent':'Mozilla/5.0'}
    f=open(CACHE,'r',encoding='utf-8')
    text=f.read()
    f.close()
    cache_dict=json.loads(text)
    try:
        soup=BeautifulSoup(cache_dict[url],'html.parser')
    except:
        text=requests.get(url,headers=headers).text
        cache_dict[url]=text
        with open(CACHE,'w',encoding='utf-8') as nf:
            nf.write(json.dumps(cache_dict))
        soup=BeautifulSoup(text,'html.parser')
    return soup

def get_results(url):
    f=open(CACHE,'r',encoding='utf-8')
    text=f.read()
    f.close()
    cache_dict=json.loads(text)
    results=None
    if url in cache_dict:
        results=cache_dict[url]
    else:
        params={'api_key':API_KEY}
        results=json.loads(requests.get(url,params=params).text).get('results')
        cache_dict[url]=results
        with open(CACHE,'w',encoding='utf-8') as nf:
            nf.write(json.dumps(cache_dict))
    return results

def scrape():
    """
    start point of scraping
    use urls, pass soup tag to Unvs
    return a list of 100 unvs(university) object
    """
    url_base='https://www.usnews.com/best-colleges/rankings/national-universities'
    unvss=[]
    for page in range(N_PAGE):
        url=url_base+'?_page={}'.format(page+1)
        soup=get_soup(url)
        unvs_tags=soup.find_all('li',id=re.compile(r'^view-.*'),class_='block-normal block-loose-for-large-up')
        for unvs_tag in unvs_tags:
            u=Unvs(unvs_tag)
            print("Collect info of {}".format(u.name))
            unvss.append(u)
    return unvss

class Unvs(object):
    def __init__(self,unvs_tag):       
        self.name=None
        self.rank=None
        self.address=None
        self.thumbnail=None
        self.n_ug=None
        self.page_url=None
        
        self.zip=None
        self.type=None
        self.year_founded=None
        self.setting=None
        self.endowment=None
        
        self.completion_rate=None
        self.cost=None
        
        if unvs_tag:
            self.scrape_overview(unvs_tag)
            assert(self.page_url!=None)
            self.scrape_detail(self.page_url)

        
        """
        print(self.name)
        print(self.rank)
        print(self.address)
        print(self.thumbnail)
        print(self.n_ug)
        print(self.page_url)
        print(self.type)
        print(self.year_founded)
        print(self.setting)
        print(self.endowment)
        """
        self.years=None
        self.ea=None
    def __repr__(self):
        return '{} in {}, ranking #'.format(self.name,self.address,self.rank)
        
    def __contains__(self,string):
        return string in self.name
        
    def scrape_overview(self,unvs_tag):
        """
        get a soup tag, scrape the basic info from tag,
        return a url directing to detailed info
        call scrpae_detail for the info
        """
        base='https://www.usnews.com'
        name_tag=unvs_tag.find('h3',class_='heading-large block-tighter').a
        assert(name_tag!=None)
        self.name=name_tag.string.strip()
        self.page_url=base+name_tag.get('href')
        assert(self.page_url!=None)
        self.address=unvs_tag.find('div',class_='block-normal text-small').string.strip()
        rank_msg=unvs_tag.find('div',style='margin-left: 2.5rem;').find('div').stripped_strings.__next__()
        match=re.search(r'\d+',rank_msg)
        assert(match)
        self.rank=int(match.group())
        self.n_ug=int(unvs_tag.find('span',string=re.compile(r'\s*Undergraduate Enrollment\s*'))\
                      .parent.strong.string.strip().replace(',',''))
        tn_tag=unvs_tag.find('a',class_='display-block right')
        if tn_tag:
            self.thumbnail=base+unvs_tag.find('a',class_='display-block right').get('href')
        
    def scrape_detail(self,url):
        """
        use the url to scrape detailed info
        """
        soup=get_soup(url)
        self.zip=soup.find('p',class_='block-normal hide-for-small-only text-small hero-ranking-data-contact').stripped_strings.__next__()[-5::1]
        if self.zip in zips:
            #print('DUPLICATE!')
        zips.append(self.zip)
        info_tags=soup.find_all('span',class_='heading-small text-black text-tight block-flush display-block-for-large-up')
        self.type=info_tags[0].string.strip()
        self.year_founded=int(info_tags[1].string.strip())
        self.setting=info_tags[4].string.strip()
        self.endowment=info_tags[5].string.strip()

    def numerize_attribute(self):
        years=CURRENT-int(self.year_founded)
        match=re.search(r'[\d\.]+',self.endowment)
        endowment=0
        if 'billion' in self.endowment:
            endowment=float(match.group())*1000
        else:
            endowment=float(match.group())
        #now endowment is in millions
        self.years=years
        self.ea=endowment
        return years,endowment
    def get_train_data(self):
        setting=['Rural','Suburban','Urban','City'].index(self.setting)
        type_=['Private, Coed','Public, Coed'].index(self.type)
        return [int(self.rank),int(self.n_ug),type_,self.years,setting,float(self.completion_rate),\
           int(self.cost),self.ea]


def mismatch_handle(unvs,words,filter1,filter2):
    if unvs.zip=='77251':
        filter2=r'school.zip={}'.format('77005')     
    if unvs.name=='Columbia University':
        words.extend(['New','York'])         
        filter1=r'school.name='+r'%20'.join(words)
    if unvs.name=='University of Virginia':
        filter2=r'school.zip={}'.format('22903')
    if unvs.name=='Wake Forest University':
        filter2=r'school.zip={}'.format('27106')
    if unvs.name=='Northeastern University':
        filter2=r'school.zip={}'.format('02115-5005')
    if unvs.name=='University of Florida':
        filter1+=r'&school.name__not=University%20of%20Florida%20Online'
    if unvs.name=='University of Miami':
        filter2=r'school.zip={}'.format('33146')
    if unvs.name=='Northeastern University':
        filter1+=r'&school.name__not=Northeastern%20University%20Professional%20Advancement%20Network'            
    if unvs.name=='Pennsylvania State University--University Park':        
        filter1=r'school.name='+r'%20'.join('Pennsylvania State University Main Campus'.split(' '))
    if unvs.name=='Ohio State University--Columbus':        
        filter1=r'school.name='+r'%20'.join('Ohio State University'.split(' '))        
    if unvs.name=='Rutgers University--New Brunswick':
        filter2=r'school.zip={}'.format('08901')
    if unvs.name=='Purdue University--West Lafayette':        
        filter1=r'school.name='+r'%20'.join('Purdue University'.split(' '))         
    if unvs.name=='Fordham University':
        filter2=r'school.zip={}'.format('10458')
    if unvs.name=='Virginia Tech':        
        filter1=r'school.name='+r'%20'.join('Virginia Polytechnic Institute and State University'.split(' '))
    if unvs.name=='Binghamton University--SUNY':
        filter1=r'school.name='+r'%20'.join('SUNY at Binghamton'.split(' '))
        filter2=r'school.zip={}'.format('13850')
    if unvs.name=='Marquette University':
        filter2=r'school.zip={}'.format('53233')
    if unvs.name=='University at Buffalo--SUNY':
        filter1=r'school.name='+r'%20'.join('SUNY Buffalo State'.split(' '))
        filter2=r'school.zip={}'.format('14222')

    #Michigan State University,University of Florida,Northeastern University need further care
    return words,filter1,filter2

def api_get(unvss):
    new_unvss=[]
    url_base=r'https://api.data.gov/ed/collegescorecard/v1/schools.json?'
    for unvs in unvss:
        tokenizer=RegexpTokenizer('\w+')
        words=tokenizer.tokenize(unvs.name)
        filter1=r'school.name='+r'%20'.join(words)
        filter2=r'school.zip={}'.format(unvs.zip)
        #mismatch handling code
        words,filter1,filter2=mismatch_handle(unvs,words,filter1,filter2)
        fields=r'_fields=school.name,2015.completion.completion_rate_4yr_150nt,2015.cost.attendance.academic_year,2015.cost.attendance.program_year'
        url=url_base+r'&'.join([filter1,filter2,fields])
        #print(url)

        results=get_results(url)
        assert(results)
        #print(results)
        if(len(results)>1):
            for i in range(10):
                #print('*****')
        #most of results have only one element
        for result in results:
            if len(results)>1 and result['school.name']!=unvs.name:
                continue      
            else:
                unvs.completion_rate=result['2015.completion.completion_rate_4yr_150nt']
                if result['2015.cost.attendance.academic_year']:
                    unvs.cost=result['2015.cost.attendance.academic_year']
                else:
                    unvs.cost=result['2015.cost.attendance.program_year']

                if unvs.cost==None:
                    for i in range(5):
                        #print("MISSING cost:",unvs.name)
                if unvs.completion_rate==None:
                    for i in range(5):
                        #print("MISSING completion rate:",unvs.name)                
        new_unvss.append(unvs)

    return new_unvss

def split(unvss):
    test_unvss=[]
    random.seed()
    for x in range(10):
        i=random.randint(0,99-x)
        test_unvss.append(unvss.pop(i))
    return unvss,test_unvss


def train_predict(unvss):
    unvss,test_unvss=split(unvss)
    dtr = AdaBoostRegressor(tree.DecisionTreeRegressor(max_depth=3),n_estimators=1000)
    #dtr=tree.DecisionTreeRegressor(max_depth=3)
    
    X=[]
    y=[]
    for unvs in unvss:
        data=unvs.get_train_data()
        X.append(data[0:-1:1])
        y.append(data[-1])
        
    test_X=[]
    test_y=[]        
    for test_unvs in test_unvss:
        test_data=test_unvs.get_train_data()
        test_X.append(test_data[0:-1:1])
        test_y.append(test_data[-1])

    dtr.fit(X,y)
    z=dtr.predict(X)
    test_z=dtr.predict(test_X)
    
    plt.figure()
    plt.scatter(y, z, c="k", marker='.', label="training set")
    plt.scatter(test_y, test_z, c="r", marker='.', label="testing set")
    plt.xlabel("actual endowment")
    plt.ylabel("prediction")
    plt.title("Adaboost Training Result")
    plt.legend()
    plt.show()

    
if __name__=='__main__':
    unvss=read_csv()
    if not unvss:
        unvss=scrape()
        unvss=api_get(unvss)
        output_csv(unvss)
    output_train(unvss)
    train_predict(unvss)
