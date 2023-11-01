import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

userpg = 'postgres'
passwordpg = 'postgres'

try:
  initpg = psycopg2.connect(
    user=userpg, password=passwordpg
  )
  initpg.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

  csr = initpg.cursor()

  #Cria a tabela (exemplo)
  csr.execute(
    """
    DROP TABLE IF EXISTS data;
    CREATE TABLE data(
      id integer NOT NULL,
      a integer NOT NULL,
      b integer NOT NULL
    );
    """
  )

  #Splita a leitura em linhas
  def leitor(file_name):
      with open(file_name, 'r') as file:
          lines = file.readlines()
      #Analisa linha por linha
      for i in range(len(lines)-1,-1,-1):
          analisa_log(lines,i)

  def analisa_log(input_list, index):
    #print(input_list[index])
    line = input_list[index]

    st = "start";  t = "<T"; cmm = "commit"
    stck = "START"; edck = "END"

    #Id. Inicio de T
    if st in line:
      print("Começo de Transação")

    #Id. T
    if t in line:
      print("Transação")

    #Id. Fim de T
    if cmm in line:
      print("Fim de Transação")

    #Id. Inicio de de Checkpoint 
    if stck in line:
      print("Começo de Checkpoint")

    #Id. Fim de Checkpoint
    if edck in line:
      print("Fim de Checkpoint")

  #Le o Log
  leitor('Arquivos/log.txt')

except (Exception, psycopg2.DatabaseError) as error:
   print(error)