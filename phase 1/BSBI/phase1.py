
from os import listdir
from os import mkdir
from os import getcwd
from os import path
from shutil import rmtree
from time import sleep


trainFilesPath= "./Dataset_IR/Train"


class IdMap:

	def __init__(self):
		
		self.str_to_id=dict()
		
		self.id_to_str=list()

	def __len__(self):
		
		return len(self.id_to_str)


	def _get_str(self,i):

		return self.id_to_str[i]

	def _get_term_id(self,term):

		if  term not in self.str_to_id:

			return -1

		return self.str_to_id[term]

	def _add_term(self,term):

		'''
			Description: 
				This function adds term and creates a mapping of
				< term , termId >
			
			Input:
				term: term to be added
		'''

		if term not in self.str_to_id:

			# len of list self.id_to_str list will be value for key term
			self.str_to_id[term] = len(self.id_to_str)

			# add word to the list self.id_to_str
			self.id_to_str.append(term)


		

class BsbiBlock:

	def __init__(self,blockSize,blockId):
		
		self.blockId=blockId
		self.blockSize= blockSize

		self.block=list()


	def get_block(self):
		return self.block

	def get_block_id(self):
		return self.blockId




	def can_tuple_be_added(self,termIdDocId):

		'''
			Description:	
				This function checks whether <termId,DocId> mapping can be added
				to the block

		'''
		# check blocks size limit

		# mappingSize=len(str(termIdDocId[0])) + len(str(termIdDocId[1]))
		mappingSize= 4 + len(str(termIdDocId[1]))

		if mappingSize <= self.blockSize:

			return True

		return False



	def add_term_doc(self,termIdDocId):

		'''
			Description:	
				This function adds <termId,DocId> mapping to the block

		'''
		# mappingSize=len(str(termIdDocId[0])) + len(str(termIdDocId[1]))
		mappingSize= 4 + len(str(termIdDocId[1]))

		self.block.append(termIdDocId)

		# reduce block size
		self.blockSize -= mappingSize

	

	def sort_block(self):

		'''
			Description:	
				This function sorts <termId,DocId> mapping 

		'''

		self.block=sorted(self.block)




class Bsbi:

	def __init__(self,trainFilesDirectory,blocksDir=None,blockSize=10000,mergedFilePath=None):

		# path to the directory of the train files  
		self.trainFilesDirectory = trainFilesDirectory

		
		# create <term,termId> mapping
		self.termIdMapping=IdMap()


		self.blocksDir=blocksDir

		# save program current direectory
		self.cwd=getcwd()

		# block size in the algorithm
		self.blockSize=blockSize

		self.mergedFilePath=mergedFilePath

		# calling init function
		self.init()


		# dictionary for merged blocks
		self.mergedBlocks=dict()




	def init(self):

		'''
			Description:
				This function initializes environment by creating directory 
				for holding blocks.

		'''

		if self.blocksDir is None:

			# make default directory for holding blocks
			self.blocksDir = self.cwd + "/" +  "blocks"

		
		# check if the directory already exist
		if	path.exists(self.blocksDir) :
			# remove dir
			rmtree(self.blocksDir)


		# create directory for holding blocks
		mkdir(self.blocksDir)



	def bsbi_invert(self,block):
		
		'''
			Description:
				This function sorts the <termID,docID> pairs, and then make them to be termID-posting.

		'''

		# termPosingList holds term->posting_list mapping 
		termPosingList=dict()

		# sort the given block
		block.sort_block()

		for ent in block.get_block():

			# if term doesnt exist in the dictionary
			if ent[0] not in termPosingList:
				termPosingList[ent[0]]= [ 1,  set([ent[1]]) ]
			else:
				# add docId to previous set
				termPosingList[ent[0]][1].add(ent[1])

				# increase document frequency
				termPosingList[ent[0]][0]+=1



		return termPosingList
		


	def write_inverted_block_to_disk(self,invertedBlock,blockId):

		'''
			Description:
				This function writes an inverted block to the disk

		'''
		fh=open(self.blocksDir + "/" +str(blockId) , 'w' )

		# blockContent will hold the content of block whoch is going to be 
		# saved on the disk 
		blockContent=str()


		for k,v in invertedBlock.items():
			blockContent+=("{item}|{df}|{docIds}\n".format(item=k,df=str(v[0]),docIds=",".join(str(i) for i in sorted(v[1]))))

		fh.write(blockContent)
		

		fh.close()






	def read_train_files(self):

		'''
			Description:
				This function reads train files

		'''

		# prompting

		print("reading files ....\n")


		# topics in the train files directory
		topics=listdir(self.trainFilesDirectory)

		
		# blockIdCounter hold block id 
		blockIdCounter=0


		# block will hold the block
		block=BsbiBlock(self.blockSize,blockIdCounter)


		for topic in topics:


			# base path for train files of topic 'topic'
			blockFilesBasePath = self.trainFilesDirectory + "/" + topic

			# name of train files of topic 'topic'
			topicFiles=listdir(blockFilesBasePath)

			# create block for holding <termId,docId>
			# block = BsbiBlock(self.blockSize,blockIdCounter)

			for file in topicFiles:

				fh=open(blockFilesBasePath + "/" + file)

				# # remove '\n' of the files line
				fileLines= fh.readlines()
				
				fh.close()

				# process the file line by line 
				for line in fileLines:

					for word in line.strip().split():

						# add term to <term,termId> mapping
						self.termIdMapping._add_term(word)

						# if the size of the block is not enough for new mapping, write previous
						# block and create new one
						if not block.can_tuple_be_added((self.termIdMapping._get_term_id(word),file)):
								
							# create invertedBlock
							invertedBlock=self.bsbi_invert(block)

							# call write_block_to_disk
							self.write_inverted_block_to_disk(invertedBlock,blockIdCounter)

							blockIdCounter+=1

							block=BsbiBlock(self.blockSize,blockIdCounter)


						# add new one to the new block
						block.add_term_doc((self.termIdMapping._get_term_id(word),file))




	def merge_blocks(self):

		'''
			Description:
				This function merges blocks and writes the final block to
				the disk

		'''

		#TODO should i write mergedBlocks again to file?

		self.mergedBlocks={}

		blocks=listdir(self.blocksDir)

		# i is blockNumber
		for i in range(0,len(blocks)):

			fh=open(self.blocksDir+"/"+str(i))
			fileLines=[ line.strip().split("|") for line in fh.readlines()]
			fh.close()

			# line[0]: termId
			# line[1]: termFrequency
			# line[2]: docId
			for line in fileLines:
				
				if line[0] not in self.mergedBlocks:
					self.mergedBlocks[line[0]]=  [ line[1],  list([line[2]]) ]

				else:
					self.mergedBlocks[line[0]][0]+=line[1]
					self.mergedBlocks[line[0]][1].append(line[2])
	
		if self.mergedFilePath is None:
			self.mergedFilePath=self.cwd+ "/" + "MergeFile"

		fh = open(self.mergedFilePath,"w")

		finalBlockContent = str()
		for k, v in self.mergedBlocks.items():
			finalBlockContent += ("{item}:{df}:{docIds}\n".format(item=k,df=str(v[0]),docIds=",".join(str(i) for i in sorted(v[1]))))

		fh.write(finalBlockContent)
		
		fh.close()
		


	def read_query(self):

		'''
			Description:
				This function reads users query

		'''

		print("to read query press r, to exit press q")
		

		while True:
			cmd = input("command: ")

			if cmd == "q":
				print("bye")
				exit(0)
			elif cmd != "r":
				print("invalid command")
				continue

			with open('query', 'r') as file:
				query = file.read()

			tokens=query.split()

			intersectedDocs = None

			for token in tokens:

				tokenId = self.termIdMapping._get_term_id(token)

				# if token was not found skip it
				if tokenId == -1:
					continue

				if intersectedDocs is None:
						
					intersectedDocs = set(self.mergedBlocks[str(tokenId)][1])
				else:

					intersectedDocs=intersectedDocs.union(set(self.mergedBlocks[str(tokenId)][1]))

			if intersectedDocs is None:
				print("nothing")
			else:
				print("results: ")
				for doc in intersectedDocs:
					print(doc)



if __name__=="__main__":
		
	main=Bsbi(trainFilesPath)


	main.read_train_files()


	main.merge_blocks()


	main.read_query()









