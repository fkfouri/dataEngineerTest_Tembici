# TESTE ENGENHEIRO DE DADOS

#### SOBRE ESTE REPOSITORIO

Este material é parte do processo de Engenheiros de Dados na Tembici, a qual foi proposto o seguinte desafio.

> ###### PARTE 01
> 
> Levando em conta duas fontes de dados sendo: um banco relacional e uma API de stream de dados,
> sua primeira missão será desenhar uma arquitetura orientada a EVENTOS para a ingestão near 
> real-time destes dados em um datalake. Justifique brevemente sua escolha levantando os pontos 
> positivos e negativos da solução.


> ###### PARTE 02
> 
> Seu objetivo é criar um script PYSPARK, que gere um arquivo único de output, utilizando os 3 
> arquivos em csv (station.csv, trip.csv e weather.csv), contendo todos os dados de viagens, 
> acrescidos de:
> 
> - Latitude/Longitude da estação de início e de fim.
> - Uma coluna com o nome "long_trip" de valor booleano sendo 'true' para viagens superiores a 10 minutos.
> - Condição meteorológia no dia da viagem (coluna events da tabela weather).
> - Uma coluna com o nome "age_range" sendo o valor de 1 para pessoas de 0-16 anos, 2 de 17-25, 3 de 26-50 e 4 para 50+.
> 


## Resolução PARTE 02

Esta resolução levou em conta dois contextos:
- processamento local (Windows)
- processamento em Cluster

### Processamento Local
A máquina utilizada possui o Sistema Operacional Windows, onde se faz necessário ter instalado o Python 3 e o Spark conforme tutorial (https://medium.com/big-data-engineering/how-to-install-apache-spark-2-x-in-your-pc-e2047246ffc3).

Toda a sequencia de execução e preparação de dados esta descrita no notebook "/local/desafio2.ipynb". Para executar basta rodar o arquivo "/local/submit_local.bat".
 
