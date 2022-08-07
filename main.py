import multiprocessing
import time
import pandas as pd


# LER ARQUIVOS PRIMEIRO
# • Matriz ordenada por linha
# • Matriz ordenada por coluna
# • Soma das linhas
# • Soma das colunas
# • Soma total da matriz
# • Maiores de cada linha
# • Maiores de cada coluna
# • Menores de cada linha
# • Menores de cada coluna




def notacaocientifico_int(string_notacao):
    string_formatada = string_notacao.replace('.', '').replace('e', '')
    lista = string_formatada.split('+')
    divisor = int(lista[0])
    expoente = int(lista[1])
    expoente = 10**expoente
    resultado = expoente*divisor
    return resultado


def ord_linha(linhas):
    lista_int = []

    elementos = linhas.split("  ")

    for elemento in elementos:
        elemento_numerico = notacaocientifico_int(elemento)
        lista_int.append(elemento_numerico)
    linhas_ordenadas = sorted(lista_int)
    return linhas_ordenadas


arquivo = open('txt.txt', 'r')
linhas = arquivo.readlines()
# print(linhas)
linhas_filtradas = []

for x in linhas:

    linhas_filtradas.append(x.strip().replace('\n', ''))
arquivo_save = open('ordenado.txt', 'w')

for linha in linhas_filtradas:
    save = ord_linha(linha)
    for x in save:
        arquivo_save.write(str(x)+'  ')

    arquivo_save.write('\n')
