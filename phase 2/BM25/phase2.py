#!/bin/python

from os import listdir
from math import log


trainFilesPath= "./Dataset_IR/Train"


class BM25:

	def __init__(self,trainFilesDirectory):

		# dictionary which maps  ( term to [ ( document , tf ) ] )
		self.termDoc=dict()

		# dictionary which maps each document to its length
		self.docLen=dict()


		self.trainFilesTerms=set()

		self.trainFiles=list()

		self.trainFilesDirectory=trainFilesDirectory

		self.trainFilesNumber=0


	def read_train_files(self):
		
		'''
			Description:
				This function reads train files

		'''

		# prompting
		print("reading files ....\n")


		# topics in the train files directory
		topics=listdir(self.trainFilesDirectory)


		for topic in topics:


			# base path for train files of topic 'topic'
			blockFilesBasePath = self.trainFilesDirectory + "/" + topic

			# name of train files of topic 'topic'
			topicFiles=listdir(blockFilesBasePath)


			for file in topicFiles:

				# increase number of train files
				self.trainFilesNumber+=1

				# add the file to list of trainFiles
				self.trainFiles.append(file)

				# create an entry for the document in the docLen
				# dictionary
				self.docLen[file]=0

				fh=open(blockFilesBasePath + "/" + file)

				# remove '\n' of the files line
				fileLines= fh.readlines()
				fh.close()

				# process the file line by line 
				for line in fileLines:

					for word in line.strip().split():

						# increase number of terms in the document
						self.docLen[file]+=1

						# increase number of terms in the whole train files
						self.trainFilesTerms.add(word)


						if word not in self.termDoc:
							
							self.termDoc[word]=[[file,1]]

						else:


							# search for the file in the list
							if file not in [ file[0] for file in self.termDoc[word] ]:
								self.termDoc[word].append([file,1])

							else:

								for i in range(len(self.termDoc[word])):
									if self.termDoc[word][i][0] == file:
										# increasing term frequency
										self.termDoc[word][i][1]+=1


	def get_term_idf(self,term):

		'''
			Description:
				This function return idf value of a term

		'''
		# number of ducuments that have term 'term'

		dft = 0 
		if term in self.termDoc:
			dft=len(self.termDoc[term])


		return log((self.trainFilesNumber-dft+0.5)/(dft+0.5)+1 , 2.718)




	def calculate_score(self,query):

		'''
			Description:
				This function calculates score of each doc according to
				the query 'query'

		'''

		# dictionary which maps the document to its score value
		# accoring to the query

		termDocScores=dict()


		for document in self.trainFiles:


			# sumVar holds the score value
			sumVar=0

			for token in query.split():

				if token not in self.termDoc:
					# if the token does not exist in the termDoc
					# dictionary , we will simply skip it 
					continue
				elif document not in [ td[0] for td in  self.termDoc[token] ]:
					# if the token does not exist in the document, again we will skip
					continue

				else:

					# calculate document frequency for term 'token'
					# td: termDocument
					frequency=sum( [ td[1] for td in self.termDoc[token] ] )

					k1=1.8
					b=0.75
					sumVar+=self.get_term_idf(token)*(frequency)*(k1+1)/(frequency+(k1*(1-b+b*self.docLen[document]/len(list(self.trainFilesTerms)))))

			# in the case of sumVar==0, we skip the document
			if sumVar!=0:
				termDocScores[document]=sumVar


		# sort the termDocScores
		return {k: v for k, v in sorted(termDocScores.items(),reverse=True, key=lambda item: item[1])}




	def read_query(self):

		'''
			Description:
				This function reads users query

		'''

		print("to exit press q")
		

		while True:
			query=input("Query: ")

			if query=="q":
				print("bye")
				exit(0)

			for k,v in self.calculate_score(query).items():

					print("doc: ",k,"  score: ",v)		


			


if __name__=="__main__":

	main=BM25(trainFilesPath)

	main.read_train_files()
		


	main.read_query()



