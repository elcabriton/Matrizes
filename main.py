import multiprocessing
import time
import pandas as pd
import numpy as np


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

COLUNA = 0
LINHA = 1

# df = pd.read_csv('txt.txt', header=None, sep='  ', engine='python')


def ord_matriz(df, ordenar=None,):
    df = pd.DataFrame(np.sort(df.values, axis=ordenar))
    return df


def soma_linhas(df):
    return df.sum(axis=1)


def soma_colunas(df):
    return df.sum(axis=0)


def soma_total(df):
    return df.sum().sum()


def maiores_linhas(df):
    return df.max(axis=1)


def maiores_colunas(df):
    return df.max(axis=0)


def menores_linhas(df):
    return df.min(axis=1)


def menores_colunas(df):
    return df.min(axis=0)


def escrever_arquivo(arquivo_entrada):
    df = pd.read_csv(arquivo_entrada, header=None, sep='  ', engine='python')
    ordenar_linha = ord_matriz(df, LINHA)
    ordenar_coluna = ord_matriz(df, COLUNA)
    sum_linha = soma_linhas(df)
    sum_coluna = soma_colunas(df)
    sum_total = soma_total(df)
    maior_linha = maiores_linhas(df)
    maior_coluna = maiores_colunas(df)
    menor_linha = menores_linhas(df)
    menor_coluna = menores_colunas(df)
    
    resultados = np.array([np.zeros(100) for _ in range(7)])

    resultados[0] = sum_linha
    resultados[1] = sum_coluna
    resultados[2, 0] = sum_total
    resultados[3] = maior_linha
    resultados[4] = maior_coluna
    resultados[5] = menor_linha
    resultados[6] = menor_coluna
    pd_resultados = pd.DataFrame(data=resultados)
    resultado_final = pd.concat([ordenar_linha, ordenar_coluna, pd_resultados])
    resultado_final = resultado_final.applymap(
        lambda number: '{:e}'.format(number))
    resultado_final.to_csv(arquivo_entrada+'_OUT.txt', sep=' ', index=False, header=False)



escrever_arquivo(r'C:\Users\mchhe\OneDrive\Imagens\Documentos\Matrizes\txt\988.txt')
