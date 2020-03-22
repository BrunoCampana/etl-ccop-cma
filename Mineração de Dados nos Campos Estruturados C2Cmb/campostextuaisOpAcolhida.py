import re
import datetime
import csv
import psycopg2

"""Conectar com o BD"""
try:
	connection = psycopg2.connect(user="postgres", password="1234",host="192.168.21.101",port="5432",database="sad")
	cursor = connection.cursor()
	postgreSQL_select_Query = "select progress_detail from sad.pln_ordr_progress where pln_ordr_id=10007200000000003098"
	cursor.execute(postgreSQL_select_Query)
	print("Selecionando textos da tabela.")
	textos = cursor.fetchall()
	
except (Exception, psycopg2.Error) as error :
	print ("Error while fetching data from PostgreSQL", error)

finally: #closing database connection.
	if(connection):
		cursor.close()
		connection.close()
		print("Conexão com PostgreSQL terminada.")
		
"""Montagem do CSV"""
			
file=open("tabelacao_opAcolhida"+str(datetime.datetime.now().day)+str(datetime.datetime.now().hour)+str(datetime.datetime.now().minute)+str(datetime.datetime.now().month)+str(datetime.datetime.now().year)+".csv","w")
writer=csv.writer(file,delimiter=",")

"""Colunas padroes do CSV"""
colunasCSV=["QUEM","ONDE","QUANDO","IMIGRANTES","CUBANOS","HAITIANOS","VENEZUELANOS","OUTROS","RESPERM","TURISTA","RESTEMP","REFUGIADOS","DESTBOAVISTA","DESTOUTRASCIDADES","DESTOUTROSPAISES","PRISÕES","CARROS","CAMINHÕES","ÔNIBUS","MOTOS","VAN","VEIOUTROS","APREENSÕES","OCORRÊNCIA","DETALHES"] #padroes no texto do operador
colunasCSV2=["QUEM","ONDE","QUANDO","IMIGRANTES","CUBANOS","HAITIANOS","VENEZUELANOS","OUTROS","RESPERM","TURISTA","RESTEMP","REFUGIADOS","DESTBOAVISTA","DESTOUTRASCIDADES","DESTOUTROSPAISES","PRISOES","CARROS","CAMINHOES","ONIBUS","MOTOS","VAN","VEIOUTROS","APREENSOES","OCORRENCIA","DETALHES"] #anterior sem acento
writer.writerow(colunasCSV2)

"""Mineração dos dados"""

for cell in textos:

#celula="Quem: 1 Pel Esp Fron(BONFIM) Onde: Receita Federal Quando: 110930NOV19 Imigrantes: 21 Cubanos: 18 Haitianos: 03 Venezuelanos: 0 Residentes Perm: 00 Residentes Temp: 00 Refugiados: 00 Destino: Boa Vista: 20 Outras Cidades: 01 Outros Países: 0 Prisões: 0 Veículos: Carros: 126 Caminhão: 0 Ônibus: 0 Motos: 18 Van: 01"
	celula=cell[0]
	#print("Texto recebido para INPUT: "+celula)
	
	"""Tratar Texto Inicial com possiveis erros do operador"""
	
	input = celula.replace("\n"," ")
	input=input.replace("Outras Cidades:","Cidades:")
	input=input.replace("Outras cidades:","Cidades:")
	input=input.replace("Residentes Perm:","Perm:")
	input=input.replace("Residentes temp:","Temp:")
	input=input.replace("Residentes perm:","Perm:")
	input=input.replace("Residentes Temp:","Temp:")
	input=input.replace("Residentes Permanentes:","Perm:")
	input=input.replace("Residentes temporários:","Temp:")
	input=input.replace("Residentes permanentes:","Perm:")
	input=input.replace("Residentes Temporários:","Temp:")
	input=input.replace("Residentes Permanente:","Perm:")
	input=input.replace("Residentes temporário:","Temp:")
	input=input.replace("Residentes permanente:","Perm:")
	input=input.replace("Residentes Temporário:","Temp:")
	input=input.replace("Boa Vista:","Vista:")
	input=input.replace("Boavista:","Vista:")
	input=input.replace("Outros Países:","Países:")
	input=input.replace("Veículos Revistados:","Veículos:")
	input=input.replace("Destino dos Imigrantes:","Destino:")
	input=input.replace("Entrada de Imigrantes:","Imigrantes:")
	input=input.replace("Dados Quantitativos:","Quantitativos:")
	input=input.replace("Turistas:","Turista:")
	input=input.replace("GDH:","Quando:")
	input=input.replace("BOA VISTA:","Vista:")
	input=input.replace("BOAVISTA:","Vista:")
	input=input.replace("Residente Perm:","Perm:")
	input=input.replace("Residente temp:","Temp:")
	input=input.replace("Residente perm:","Perm:")
	input=input.replace("Residente Temp:","Temp:")
	input=input.replace("Residente Permanentes:","Perm:")
	input=input.replace("Residente temporários:","Temp:")
	input=input.replace("Residente permanentes:","Perm:")
	input=input.replace("Residente Temporários:","Temp:")
	input=input.replace("Residente Permanente:","Perm:")
	input=input.replace("Residente temporário:","Temp:")
	input=input.replace("Residente permanente:","Perm:")
	input=input.replace("Residente Temporário:","Temp:")
	#print("INPUT tratado: "+input)

	linhas=re.split("[^\s]*:",input)
	colunas=re.findall("[^\s]*:",input)
	resultados=[]
	#print("linhas")
	#print(linhas)
	#print("colunas")
	#print(colunas)
	
	"""Tratamento das colunas"""
	for i in range(len(colunas)):
		colunas[i]=colunas[i][:-1]
		if(colunas[i]=="Vista"):
			colunas[i]="DestBoaVista"
		if(colunas[i]=="Cidades"):
			colunas[i]="DestOutrasCidades"
		if(colunas[i]=="Temp"):
			colunas[i]="ResTemp"
		if(colunas[i]=="Países"):
			colunas[i]="DestOutrosPaises"
		if(colunas[i]=="Perm"):
			colunas[i]="ResPerm"		
					
	"""Tratamento das linhas"""
	for i in range(len(linhas)):
		linhas[i]=linhas[i].replace("S/A","0")
		if (i==0):
			pass
		elif (i==len(linhas)-1):
			resultados.append(linhas[i])
		else:
			resultados.append(linhas[i][:-1])
			
	#print("colunas tratadas")
	#print(colunas)	
	#print("linhas tratadas")
	#print(resultados)
	
	"""Organizar informações na ordem prevista do CSV"""

	erro=False
	
	final=["0"]*25	
	for i in range(len(final)):
		for j in range(len(colunas)):
			if (colunas[j].lower()==colunasCSV[i].lower()):
				if(j==0 or j==1 or j==2 or j==24 or j==23):
					final[i]=resultados[j]
				else:
					aux=resultados[j].replace(" ","")
					try:
						final[i]=str(int(aux))
					except:
						erro=True
						print("\n==========ERRO==========")
						print("Erro no formato do texto de entrada: "+celula)
						""""print("Não int no campo "+str(j)+" no texto : "+celula)
						indice=aux.find("(")
						final[i]=aux[0:indice]
						print(aux[0:indice])"""
					#final[i]=aux
	
	#print("final tratadas")
	#print(final)

	"""Consertar Data para o formato civil"""
	
	meses = {
		"JAN":"01",
		"FEV":"02",
		"MAR":"03",
		"ABR":"04",
		"MAI":"05",
		"JUN":"06",
		"JUL":"07",
		"AGO":"08",
		"SET":"09",
		"OUT":"10",
		"NOV":"11",
		"DEZ":"12"
	}

	try:
		final[2]=final[2].replace(" ","")

		if (len(final[2]) > 10):

			dia=final[2][0:2]
			mes=final[2][6:9]
			mes=meses[mes.upper()]
			ano=final[2][9:11]
			hora=final[2][2:4]
			minutos=final[2][4:6]
			final[2]=ano+"/"+mes+"/"+dia+" "+hora+":"+minutos+":00"

		elif (len(final[2])==7): #01SET19
			dia=final[2][0:2]
			mes=final[2][2:5]
			mes=meses[mes.upper()]
			ano=final[2][5:7]
			final[2]=ano+"/"+mes+"/"+dia
		else:
			erro=True
			print("\n==========ERRO==========")
			print("Erro na Data: "+celula)

	except:
		erro=True
		print("\n==========ERRO==========")
		print("Erro no formato do texto de entrada: "+celula)


	"""Escrever linha no arquivo"""
	if (not erro):
		#print(celula)
		writer.writerow(final)
	
	#print("csv final")
	#print(final)

file.close()

