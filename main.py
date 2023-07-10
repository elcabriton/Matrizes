import multiprocessing
import time
import pandas as pd
import numpy as np


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


def escrever_arquivo(arquivo_entrada):
    df = ler_arquivo(arquivo_entrada)
    ordenar_linha = ord_matriz(df, LINHA)
    ordenar_coluna = ord_matriz(df, COLUNA)
    
    # Chamar a função "calculos"
    resultados = calculos(df)

    pd_resultados = pd.DataFrame(data=resultados)
    resultado_final = pd.concat([ordenar_linha, ordenar_coluna, pd_resultados])
    resultado_final.to_csv(arquivo_entrada+'_OUT.txt', sep=' ', index=False, header=False)


def calculos(df):
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

    return resultados


def processar_arquivos(arquivos, num_threads_ler, num_threads_escrever, num_threads_calculos):
    tempos_leitura = []
    tempos_calculo = []
    tempos_escrita = []
    tempos_arquivo = []

    processos = []

    for arquivo in arquivos:
        inicio_arquivo = time.time()
        df = ler_arquivo(arquivo)
        fim_leitura = time.time()
        tempos_leitura.append(fim_leitura - inicio_arquivo)

        ordenar_linha = ord_matriz(df, LINHA)
        ordenar_coluna = ord_matriz(df, COLUNA)

        inicio_calculo = time.time()
        resultados = calculos(df)
        fim_calculo = time.time()
        tempos_calculo.append(fim_calculo - inicio_calculo)

        pd_resultados = pd.DataFrame(data=resultados)
        resultado_final = pd.concat([ordenar_linha, ordenar_coluna, pd_resultados])

        inicio_escrita = time.time()
        resultado_final.to_csv(arquivo+'_OUT.txt', sep=' ', index=False, header=False)
        fim_escrita = time.time()
        tempos_escrita.append(fim_escrita - inicio_escrita)

        tempos_arquivo.append(fim_escrita - inicio_arquivo)

    for processo in processos:
        processo.join()

    for i, arquivo in enumerate(arquivos):
        print(f"Tempo para o arquivo {arquivo}:")
        print("Tempo de leitura:", tempos_leitura[i])
        print("Tempo de cálculo:", tempos_calculo[i])
        print("Tempo de escrita:", tempos_escrita[i])
        print("Tempo total:", tempos_arquivo[i])
        
    tempo_total_de_leitura = sum(tempos_leitura)
    tempo_total_de_calculos = sum(tempos_calculo)
    tempo_total_de_escrita = sum(tempos_escrita)
    tempo_total_arquivos = sum(tempos_arquivo)

    print("Tempo total para todas as leituras:", tempo_total_de_leitura)
    print("Tempo total para todos os cálculos:", tempo_total_calculos)
    print("Tempo total para todas as escritas:", tempo_total_escrita)
    
    print("Tempo total para todos os arquivos:", tempo_total_arquivos)


if __name__ == "__main__":
    
    

    arquivos = ['txt/'+str(i)+'.txt' for i in range(1, 31)]

    num_threads_ler = int(input("Digite o número de threads para a função ler_arquivo: "))
    num_threads_escrever = int(input("Digite o número de threads para a função escrever_arquivo: "))
    num_threads_calculos = int(input("Digite o número de threads para a função calculos: "))

    processar_arquivos(arquivos, num_threads_ler, num_threads_escrever, num_threads_calculos)

   