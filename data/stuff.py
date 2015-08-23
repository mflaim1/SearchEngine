from google import search
import sqlite3
import urllib.request
import urllib.response
import urllib.parse
from bs4 import BeautifulSoup
import nltk
import math
from nltk.tokenize import RegexpTokenizer
import re
import os, socket
import pickle
import operator
from nltk.stem.porter import *
#created by Mariah Flaim and  Colleen Kimball

class Spider:
    title=""
    pageText=""
    tokens=[]
    def __init__(self, urlIn):
        self.title="not found yet"
        self.pageText="page text not yet found"
        self.tokens=["no","tokens","yet"]
        self.terms=["no","terms","yet"]
        self.url=urlIn
        self.header=""
        self.mod = ""
        self.desc=""
        self.dictDictList={}
    def download(self):
        """
            This function tries to open a url using a user agent Opera, basically tricking server into thinking
            we are using Opera to open the site. It downloads the raw html on the page, granted that there are no
            HTTP or socket errors while running the code.
        """
        try:
            u = urllib.request.URLopener() # Python 3: urllib.request.URLOpener
            u.addheaders = []
            u.addheader('User-Agent', 'Opera/9.80 (Windows NT 6.1; WOW64; U; de) Presto/2.10.289 Version/12.01')
            u.addheader('Accept-Language', 'de-DE,de;q=0.9,en;q=0.8')
            u.addheader('Accept', 'text/html, application/xml;q=0.9, application/xhtml+xml, image/png, image/webp, image/jpeg, image/gif, image/x-xbitmap, */*;q=0.1')
            f = u.open(self.url)
            thePage = f.read()
            self.header=f.info()
            if f.info()['Last-Modified']:
                self.mod=f.info()['Last-Modified']
            self.pageText=thePage.decode("iso-8859-1")
            f.close()
        except (urllib.error.HTTPError):
            import sys; ty, err, tb = sys.exc_info()
            print("HTTP Error.")
        except socket.error:
            import sys; ty, err, tb = sys.exc_info()
            print("Socket Error.")

    def parser(self):
        """
            This function uses BeautifulSoup to parse the raw html text of the page and cleans it up. It then uses nltk to
            tokenize the page text.
        """
        soup = BeautifulSoup(self.pageText)
        for script in soup(["script", "style"]):
            script.extract()
        if soup.title!=None:
            self.title=(soup.title.string)
        # First get the meta description tag
        d = soup.find('meta', attrs={'name':'og:description'}) or soup.find('meta', attrs={'property':'description'}) or soup.find('meta', attrs={'name':'description'})
        if d:
            self.desc =d.get('content')
        # If description meta tag was found, then get the content attribute and save it to db entry
        self.pageText=(soup.get_text())
        self.removeHtmlComments()
        self.removeComments()
        self.removePunct()
        self.tokens=nltk.word_tokenize(self.pageText)



    def stemTerms(self):
        """
            This function goes through the list of tokens and stems them. It also consolidates the list of stems to only
            contain one copy of each stem
        """
        stemIt=PorterStemmer()
        for i in range(len(self.tokens)):
            self.terms.append(stemIt.stem(self.tokens[i]))

    def removePunct(self):
        """
            This function removes punctuation from the page text
        """
        self.pageText= re.sub(re.compile("[^'\w\s]",re.DOTALL ) ,"" ,self.pageText) # remove all occurance punctuation
    def removeComments(self):
        """
            This function removes javaScript comments from the page text
        """
        self.pageText = re.sub(re.compile("/\*.*?\*/",re.DOTALL ) ,"" ,self.pageText) # remove all occurance streamed comments (/*COMMENT */) from string
        self.pageText = re.sub(re.compile("//.*?\n" ) ,"" ,self.pageText) # remove all occurance singleline comments (//COMMENT\n ) from string

    def removeHtmlComments(self):
        """
            This function removes html comments from the page text
        """
        self.pageText = re.sub(re.compile("<!--.*?-->",re.DOTALL ) ,"" ,self.pageText) # remove all occurance streamed comments (/*COMMENT */) from string

    def lower(self):
        """
            This function turns all of the terms into lowercase text. It also checks to see if the term is a multiple, and if it is,
            it consolidates the list.
        """
        for term in self.terms:
            term=term.lower()

class Crawler:

    def __init__(self, Nin):
        self.N = Nin
        self.conn=sqlite3.connect('data/cache.db')
        self.c = self.conn.cursor()
        self.cache()

    def search(self, items):
        """
            This function searches through the text files of the items given in the list that is the parameter items.
            For each item in each text file, it opens the top ten search results on google and records the page information.
        """
        i=1
        for item in items:
            #open the item text file and read in the items to search
            f = open("data/item/"+item+".txt", "r", encoding= "utf-8")
            lines=f.readlines()
            for line in lines:
                #insert each item into the Item Table
                itemId=self.insertItem(line[:-1],item)
                count=0
                if line[len(line)-1]=="\n":
                    line=line[:-1]
                for url in search("\""+line+"\" "+ item, stop=self.N*2):
                    #count the number of pages up to N so you don't lose if HTTP error
                    if count<self.N:
                        count+=1
                        string = str(i)
                        #create the filename (6 integer string)
                        for j in range(6-len(str(i))):
                            string = '0'+string

                        #enter the url into the db
                        urlId=self.insertCachedURL(url, string)
                        self.insertURLToItem(urlId,itemId)
                        i+=1
                    else:
                        #break if you've already got 10 pages
                        break
            self.conn.commit()


    def _quote(self, text):
        """
        Properly adjusts quotation marks for insertion into the database.
        """
        text = re.sub("'", "''", text)
        return text


    def _unquote(self, text):
        """
        Properly adjusts quotations marks for extraction from the database.
        """
        text = re.sub("''", "'", text)
        return text


    def cache(self):
        self.c.execute("CREATE TABLE IF NOT EXISTS URL( Id integer primary key AutoIncrement not null , url string not null UNIQUE, dateMod string, title string, desc string);")
        self.c.execute("CREATE TABLE IF NOT EXISTS Item( Id integer primary key AutoIncrement not null UNIQUE, name string not null, type string not null);")
        self.c.execute("CREATE TABLE IF NOT EXISTS URLToItem (Id INTEGER PRIMARY KEY AutoIncrement, urlID INTEGER, itemID INTEGER);")
        self.conn.commit()


    def lookupCachedURL_byURL(self, url):
        """
        Returns the id of the row matching url in CachedURL.
        If there is no matching url, returns an None.
        """
        sql = "SELECT id FROM URL WHERE URL='%s'" % (self._quote(url))
        res = self.c.execute(sql)
        reslist = res.fetchall()
        if reslist == []:
            return None
        elif len(reslist) > 1:
            raise RuntimeError('DB: constraint failure on CachedURL.')
        else:
            return reslist[0][0]


    def lookupCachedURL_byID(self, cache_url_id):
        """
        Returns a (url, docType, title) tuple for the row
        matching cache_url_id in CachedURL.
        If there is no matching cache_url_id, returns an None.
        """
        sql = "SELECT url, docType, title FROM URL WHERE id=%d"\
              % (cache_url_id)
        res = self.c.execute(sql)
        reslist = res.fetchall()
        if reslist == []:
            return None
        else:
            return reslist[0]


    def lookupItem(self, name, itemType):
        """
        Returns a Item ID for the row
        matching name and itemType in the Item table.
        If there is no match, returns an None.
        """
        sql = "SELECT Id FROM Item WHERE name='%s' AND type='%s'"\
              % (self._quote(name), self._quote(itemType))
        res = self.c.execute(sql)
        reslist = res.fetchall()
        if reslist == []:
            return None
        else:
            return reslist[0][0]


    def lookupURLToItem(self, urlId, itemId):
        """
        Returns a urlToItem.id for the row
        matching name and itemType in the Item table.
        If there is no match, returns an None.
        """
        if urlId and itemId:
            sql = "SELECT Id FROM URLToItem WHERE urlID='%d' AND itemID='%d'"\
                  % (urlId, itemId)
            res = self.c.execute(sql)
            reslist = res.fetchall()
            if reslist == []:
                return None
            else:
                return reslist[0]
        return None


    def deleteCachedURL_byID(self, cache_url_id):
        """
        Delete a CachedURL row by specifying the cache_url_id.
        Returns the previously associated URL if the integer ID was in
        the database; returns None otherwise.
        """
        result = self.lookupCachedURL_byID(cache_url_id)
        if result == None:
            return None
        (url, download_time, docType) = result
        sql = "DELETE FROM URL WHERE id=%d" % (cache_url_id)
        self.c.execute(sql)
        return url


    def insertCachedURL(self, url, string):
        """
        Inserts a url into the CachedURL table, returning the id of the
        row.
        Enforces the constraint that url is unique.
        Writes raw html and header to file
        """
        #if the url is already in the table, you don't need to insert it
        cache_url_id = self.lookupCachedURL_byURL(url)
        if cache_url_id is not None:
            return cache_url_id

        #make a spider for the url
        s = Spider(url)
        s.download()
        #open raw html file to write to
        rawFile = open("data/raw/"+string+".html","w",  encoding= "utf-8")
        rawFile.write(s.getPageText())
        #open the header file to write to
        header = open("data/header/"+string+".txt","w",encoding="utf-8")
        header.write(str(s.header)+"\n")
        s.lower()
        s.parser()
        #insert url into table
        res = self.c.execute("""INSERT INTO URL (url, dateMod, title, desc)
               VALUES (?,?,?,?)""", (url, s.mod, s.title, s.desc))
        self.conn.commit()
        s.stemTerms()
        clean = open("data/clean/"+string+".txt","w",encoding="utf-8")
        for t in s.getTerms():
            clean.write(str(t)+"\n")
        #return the id of the url
        return self.c.lastrowid


    def insertItem(self, name, itemType):
        """
        Inserts a item into the Item table, returning the id of the
        row.
        itemType should be something like "music", "book", "movie"
        Enforces the constraint that name is unique.
        """
        item_id = self.lookupItem(name, itemType)
        if item_id is not None:
            return item_id
        sql = """INSERT INTO Item (name, type)
              VALUES (\'%s\', \'%s\')""" % (self._quote(name), (self._quote(itemType)))
        res = self.c.execute(sql)
        return self.c.lastrowid

    def insertURLToItem(self, urlID, itemID):
        """
        Inserts a item into the URLToItem table, returning the id of the
        row.
        Enforces the constraint that (urlID,itemID) is unique.
        """
        u2i_id = self.lookupURLToItem(urlID, itemID)
        if u2i_id is not None:
            return u2i_id
        sql = """INSERT INTO URLToItem (urlID, itemID)
                 VALUES (\'%d\', \'%d\')""" % (urlID, itemID)
        res = self.c.execute(sql)
        return self.c.lastrowid





class Query():
    def __init__(self):
        self.dict={}
        self.docLen={}
        self.queryTF={}
        self.queryScores={}
        self.conn=sqlite3.connect('data/cache.db')
        self.c = self.conn.cursor()


    def getDict(self):
        #uncomment to build dictionary again
        '''
        self.c.execute("SELECT id from URL;")

        for row in self.c.fetchall():
            id=row[0]
            strId=str(id)
            if id<=390:
                for j in range(6-len(strId)):
                    strId = '0'+strId

                f=open("data/clean/"+strId+".txt","r", encoding="utf-8")
                lines=f.readlines()

                for i in range(len(lines)):
                    line=lines[i][:-1].lower()
                    if line not in self.dict:
                        self.dict[line]={id:[-1]}
                        self.dict[line][id].append(i)
                    elif id not in self.dict[line]:
                        self.dict[line][id]=[-1]
                        self.dict[line][id].append(i)
                    else:
                        self.dict[line][id].append(i)
                pickle.dump(self.dict, open("save.p","wb"))
        '''
        self.dict = pickle.load( open( "save.p", "rb" ) )

    def getDocLTC(self):
        #collect normalized inverse doc frequencies and term frequencies and
        # multiply together for weight of each webpage per term
        self.c.execute("select id from URL;")
        num=len(self.c.fetchall()) #number of docs in index
        self.docLen={}
        for term in self.dict:
            for docId in self.dict[term]:
                self.dict[term][docId][0]=(1+(math.log10(len(self.dict[term][docId])-1))*(math.log10(num/(len(self.dict[term].keys())))))
                if docId not in self.docLen:
                    self.docLen[docId]=(self.dict[term][docId][0])*(self.dict[term][docId][0])
                else:
                    self.docLen[docId]+=(self.dict[term][docId][0])*(self.dict[term][docId][0])

        #update weight by dividing by square root of normalized docLength
        for term in self.dict:
            for docId in self.dict[term]:
                self.dict[term][docId][0]=(self.dict[term][docId][0])/(math.sqrt(self.docLen[docId]))


    def getDocNNN(self):
        #gets inverse doc frequency for each term
        #gets term frequency for each terms for each document
        #no normalization
        for term in self.dict:
            for docId in self.dict[term]:
                self.dict[term][docId][0]=(len(self.dict[term][docId])-1)

    def getQueryTF(self, query):
        #get term frequency in a query
        self.queryTF={}
        query=query.lower()
        query=query.split()
        stemIt=PorterStemmer()
        stemmed=[]
        for q in query:
            q=stemIt.stem(q)
            stemmed.append(q)
        for term in stemmed:
            if term not in self.queryTF:
                self.queryTF[term]=1
            else:
                self.queryTF[term]+=1

    def getQueryNNN(self,query):
        #no normalization
        self.queryScores={}
        self.getQueryTF(query)
        noTerm=0
        badTerms=[]

        for term in self.queryTF:
            if term in self.dict.keys():
                for docId in self.dict[term]:
                    if docId not in self.queryScores:
                        self.queryScores[docId]=(self.queryTF[term])*(self.dict[term][docId][0])
                    else:
                        self.queryScores[docId]+=(self.queryTF[term])*(self.dict[term][docId][0])
            else:
                print("Term not in documents, results will not contain this term: "+term)
                noTerm=noTerm+1
                badTerms.append(term)
        for term in badTerms:
            del self.queryTF[term]
        if len(self.queryTF)==0:
            print("No results in our database for this query")
        else:
            self.getTopFive(self.queryScores)


    def getQueryLTC(self,query):
        #normalize query
        self.c.execute("select id from URL;")
        num=len(self.c.fetchall()) #num of docs in index
        self.queryScores={}
        self.getQueryTF(query)
        noTerm=0
        badTerms=[]
        qLength = 0
        for term in self.queryTF:
            print(term)
            if term in self.dict.keys():
                self.queryTF[term]=(1+math.log10(self.queryTF[term]))*(math.log10(num/(len(self.dict[term].keys()))))
                qLength += (self.queryTF[term])*(self.queryTF[term])
            else:
                #let user know that results will not contain ter,
                badTerms.append(term)
                print("term not in documents, results will not contain this term: "+term)
                noTerm=noTerm+1


        for term in badTerms:
            #delete terms that arent in docs from query
            del self.queryTF[term]

        if len(self.queryTF)==0:
            #check if all terms in query were not in docs
            print("No results in our database for this query")
        else:

            for term in self.queryTF:
                self.queryTF[term]  = self.queryTF[term]/math.sqrt(qLength)

                for docId in self.dict[term]:
                    if docId not in self.queryScores:
                        self.queryScores[docId]=(self.queryTF[term])*(self.dict[term][docId][0])
                    else:
                        self.queryScores[docId]+=(self.queryTF[term])*(self.dict[term][docId][0])

            self.getTopFive(self.queryScores)


    def getTopFive(self,docs):
        #sort list of documents and print out the top five with highest weights
        sorted_docs = sorted(docs.items(), key=operator.itemgetter(1))
        allDocs=[x[0] for x in sorted_docs]
        topFive=[]
        for i in range(1,6):
            topFive.append(sorted_docs[len(dict(sorted_docs).keys())-i])

        sorted_docs.reverse()

        topDocs=[x[0] for x in sorted_docs]
        topWeights=[x[1] for x in sorted_docs]

        self.getUrlsFromDocIdList(topDocs,topWeights, allDocs)

    def getUrlsFromDocIdList(self, topDocs, topWeight, allDocs):
        itemDict={}
        #fetch info for top five results from database
        print("TOP FIVE RESULTS: ")
        for i in range(1,6):
            self.c.execute("SELECT itemID FROM URLtoItem WHERE urlID="+str(topDocs[i])+";")
            itemID=self.c.fetchone()[0]
            self.c.execute("SELECT name, type FROM Item WHERE id="+str(itemID)+";")
            name, type = self.c.fetchone()
            self.c.execute("SELECT title, url FROM URL WHERE id="+str(topDocs[i])+";")
            row=self.c.fetchone()
            if row[0] and row[1] and name and type:
                print("Doc "+str(topDocs[i])+" (weight:"+str(topWeight[i])+")"+"\t"+row[0]+"\t"+row[1]+"\t"+type+"\t"+name)
        #found out which specific top 5 items are the most common in your results
        for i in range(len(topDocs)):
            self.c.execute("SELECT itemID FROM URLtoItem WHERE urlID="+str(topDocs[i])+";")
            itemID=self.c.fetchone()[0]
            self.c.execute("SELECT name, type FROM Item WHERE id="+str(itemID)+";")
            name, type = self.c.fetchone()
            if name:
                if name in itemDict:
                    itemDict[name]+=topWeight[i]
                else:
                    itemDict[name]=topWeight[i]

        count=0
        sorted_items = sorted(itemDict.items(), key=operator.itemgetter(1))
        sorted_items.reverse()
        print("Most Common Items: \n")
        for i in sorted_items:
            if count==5:
                break
            count+=1
            print(str(count)+": "+i[0]+" with "+str(i[1]))



def main():
    #uncomment to rebuild dataset
    '''
    c =Crawler(10)
    c.search(["books","movies","music"])
    c.c.close()
    '''


    q = Query()
    q.getDict()
    print("Welcome to our Search Engine! \n")
    docW=input("Please enter the document weighting scheme (ltc or nnn): ")
    docW=docW.lower()
    while docW!="ltc" and docW!="nnn":
        docW=input("Invalid Input: Please enter ltc or nnn: ")

    queryW=input("Please enter the query weighting scheme (ltc or nnn): ")
    queryW=queryW.lower()
    while queryW!="ltc" and queryW!="nnn":
        queryW=input("Invalid Input: Please enter ltc or nnn: ")

    print("Loading document index...")
    if docW=="ltc":
        q.getDocLTC()
    else:
        q.getDocNNN()

    while True:
        query=input("Please Enter a query or 'Q' to Exit: ")
        if query=="Q":
            print("Goodbye!")
            break
        elif queryW=="ltc":
            q.getQueryLTC(query)
        else:
            q.getQueryNNN(query)



main()