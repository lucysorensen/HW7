import sqlalchemy
import csv

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, and_, or_
from sqlalchemy.orm import relationship, backref, sessionmaker

Base = declarative_base() 
  
class Source(Base):
  __tablename__ = 'sources'
  
  id = Column(Integer, primary_key=True)
  name = Column(String)
  url = Column(String)
  scrapes = relationship("Scrape")
  
  def __init__(self, name, url):
    self.name = name
    self.url = url
  
  def __repr__(self):
    return "<source('%s')>" % (self.name)

class Scrape(Base):
	
	__tablename__ = 'scrapes'
	id = Column(Integer, primary_key=True)
	is_post = Column(Integer)
	publish_date = Column(String)
	author = Column(String)
	url = Column(String)
	post_title = Column(String)
	comment_count = Column(Integer)
	source_id = Column(Integer, ForeignKey('sources.id'))
	
	def __init__(self, filename, rownum, source_id = None):
		self.filename = filename
		readFile = open(self.filename, "rb")
		self.csvreader = csv.reader(readFile)
		matrix = []
		for row in self.csvreader:
			matrix.append(row)
		entry = matrix[rownum-1]
		self.is_post = entry[0]
		self.publish_date = entry[1]
		self.author = entry[2]
		self.url = entry[3]
		self.post_title = entry[4]
		self.comment_count = entry[5]
		self.source_id = source_id

	def __repr__(self):
		return "<scrape('%s')>" % (self.post_title)	
		
class User(Base):
	
	__tablename__ = 'users'
	id = Column(Integer, primary_key=True)
	user_id = Column(Integer)
	screen_name = Column(String)
	num_followers = Column(Integer)
	crawl_id = Column(Integer, ForeignKey('crawls.id'))
	
	def __init__(self, filename, rownum, crawl_id = None):
		self.filename = filename
		readFile = open(self.filename, "rb")
		self.csvreader = csv.reader(readFile)
		matrix = []
		for row in self.csvreader:
			matrix.append(row)
		entry = matrix[rownum-1]
		self.user_id = entry[0]
		self.screen_name = entry[1]
		self.num_followers = entry[2]

	def __repr__(self):
		return "<user('%s')>" % (self.screen_name)	
		
class Crawl(Base):
	
	__tablename__ = 'crawls'
  
	id = Column(Integer, primary_key=True)
	start_id = Column(Integer)
	start_time = Column(String)
	users = relationship('User')
	
	def __init__(self, filename, rownum):
		readFile = open(filename, "rb")
		self.csvreader = csv.reader(readFile)
		matrix = []
		for row in self.csvreader:
			matrix.append(row)
		entry = matrix[rownum-1]
		self.start_id = entry[0]
		self.start_time = entry[1]
	
	def __repr__(self):
		return "<crawl('%s at %s')>" % (self.start_id, self.start_time)

def MakeOtterDatabase():
		filename = 'hw5_results.csv'
		engine = sqlalchemy.create_engine('sqlite:///blogscrapes.db', echo=True)
		Base.metadata.create_all(engine) 
		Session = sessionmaker(bind = engine)
		session = Session()
		source1 = Source('The Daily Otter', 'http://dailyotter.org/')
		str(source1.id)
		i=2
		while i in range(2, 21):
			row = Scrape(filename, i, 1)
			str(row.id)
			session.add(row)
			source1.scrapes.append(row)
			i+=1
		session.add(source1)
		session.commit()
# 		for j in session.query(Scrape).order_by(Scrape.publish_date):
# 			print j.post_title, j.publish_date
		
def MakeTwitterDatabase():
		filename1 = 'hw6_users.csv'
		filename2 = 'hw6_crawls.csv'
		engine = sqlalchemy.create_engine('sqlite:///twittercrawls.db', echo=True)
		Base.metadata.create_all(engine) 
		Session = sessionmaker(bind = engine)
		session = Session()
		crawl1 = Crawl(filename2, 1)
		str(crawl1.id)
		i=2
		while i < 200:
			row = User(filename1, i, 1)
			str(row.id)
			session.add(row)
			crawl1.users.append(row)
			i+=1
		session.add(crawl1)
		session.commit()
# 		for j in session.query(User).order_by(User.num_followers):
# 			print j.screen_name, j.num_followers

MakeOtterDatabase()
MakeTwitterDatabase()
