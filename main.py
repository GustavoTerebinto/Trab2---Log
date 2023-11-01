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