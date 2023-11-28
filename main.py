import json
import sys
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

  #Leitura do arquivo de metadados
  def leitor_meta(file_name):
    file = open(file_name, 'r')
    data = json.load(file)["table"]
    data_table = list(zip(data["id"], data["A"], data["B"]))

    for i in data_table:
      csr.execute(f"INSERT INTO data VALUES ({i[0]},{i[1]},{i[2]})")

  #Splita a leitura em linhas
  def leitor(file_name):
      st_Tr = []; cm_Tr = []; ck_Tr = []
      flag_ck = False
      x = 0

      with open(file_name, 'r') as file:
          lines = file.readlines()
      #Analisa linha por linha
      for i in range(len(lines)-1,-1,-1):
          x = analisa_log(lines, i, st_Tr, cm_Tr, ck_Tr, flag_ck)
          if (x == 1): break

      #Saida
      for i in st_Tr:
        if i not in cm_Tr:
          print(f"Transação {i} realizou UNDO")
      initpg.commit()
      print("\nID\tA\tB")
      csr.copy_to(sys.stdout, "data", sep="\t")

  def analisa_log(input_list, index, start, commit, checkpoint, flag):
    #print(input_list[index])
    line = input_list[index]

    st = "start";  t = "<T"; cmm = "commit"
    stck = "START"; edck = "END"

    #Id. Inicio de T
    if st in line:
      start.append(line[7]+line[8])
      #print(start)

    #Id. T
    if t in line:
      tr = line[1]+line[2]

      line_n = line.replace("<", "").replace(">", "")
      if "\t\n" in line_n:
         line_n = line.replace("<","").replace(">\t\n", "")
      if "\n" in line_n:
         line_n = line.replace("<","").replace(">\n", "")
      line_tuple = line_n.split(",")
      
      #print(line_tuple)
      if tr in commit:
         csr.execute(f"SELECT {line_tuple[2]}  FROM data WHERE id = {line_tuple[1]}")
         dbval = csr.fetchone()[0]
         if str(dbval) != str(line_tuple[3]):
          csr.execute(f"UPDATE data SET {line_tuple[2]}={line_tuple[3]} WHERE id={line_tuple[1]}")

    #Id. Fim de T
    if cmm in line:
      commit.append(line[8]+line[9])
      #print(commit)

    #Id. Inicio de de Checkpoint 
    if stck in line:
      flag = True
      tr = line.split('(')[1].replace(")>\n", "").replace(" ", '')
      checkpoint = tr.split(",")


    #Id. Fim de Checkpoint
    if flag:
      if len(checkpoint) == 0: return 1

  #Le o metadados
  leitor_meta('Arquivos/metadado.json')

  #Le o Log
  leitor('Arquivos/log.txt')

except (Exception, psycopg2.DatabaseError) as error:
   print(error)