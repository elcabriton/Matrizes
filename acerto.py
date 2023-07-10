import time
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor

# LER ARQUIVOS PRIMEIRO
# • Matriz ordenada por linha
# • Matriz ordenada por coluna

COLUNA = 0
LINHA = 1


def ler_arquivo(arquivo_entrada):
    df = pd.read_csv(arquivo_entrada, header=None, sep='  ', engine='python')
    return df


def ord_matriz(df, ordenar=None):
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


def gerar_resultados(df):
    ordenar_linha = ord_matriz(df, LINHA)
    ordenar_coluna = ord_matriz(df, COLUNA)
    sum_linha = soma_linhas(df)
    sum_coluna = soma_colunas(df)
    sum_total = soma_total(df)
    maior_linha = maiores_linhas(df)
    maior_coluna = maiores_colunas(df)
    menor_linha = menores_linhas(df)
    menor_coluna = menores_colunas(df)

    resultados = np.array([np.zeros(1000) for _ in range(7)])

    resultados[0] = sum_linha
    resultados[1] = sum_coluna
    resultados[2, 0] = sum_total
    resultados[3] = maior_linha
    resultados[4] = maior_coluna
    resultados[5] = menor_linha
    resultados[6] = menor_coluna

    return ordenar_linha, ordenar_coluna, resultados


def gerar_saida(arquivo_entrada, ordenar_linha, ordenar_coluna, resultados):
    pd_resultados = pd.DataFrame(data=resultados)
    resultado_final = pd.concat([ordenar_linha, ordenar_coluna, pd_resultados])
    resultado_final = resultado_final.applymap(
        lambda number: '{:e}'.format(number))
    resultado_final.to_csv(arquivo_entrada+'_OUT.txt',
                           sep=' ', index=False, header=False)


if __name__ == "__main__":
    inicio = time.time()
    num_workers = 8

    # Step 1: Read files
    arquivos = ['txt/'+str(i)+'.txt' for i in range(1, 31)]
    with ThreadPoolExecutor(max_workers=num_workers) as file_executor:
        dfs = list(file_executor.map(ler_arquivo, arquivos))

    # Step 2: Perform calculations
    with ThreadPoolExecutor(max_workers=num_workers) as calc_executor:
        resultados = list(calc_executor.map(gerar_resultados, dfs))

    # Step 3: Generate output files
    with ThreadPoolExecutor(max_workers=num_workers) as output_executor:
        for i in range(len(arquivos)):
            output_executor.submit(gerar_saida, arquivos[i], *resultados[i])

    fim = time.time()
    print(fim-inicio)
