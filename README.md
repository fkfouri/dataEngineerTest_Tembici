# TESTE ENGENHEIRO DE DADOS

#### SOBRE NÓS

Desde 2010, a Tembici. é uma empresa especializada em soluções de mobilidade urbana através do uso das bicicletas como meio de transporte. Hoje somos a maior empresa de micromobilidade da América Latina. Temos projetos em 14 cidades na América Latina, nas grandes cidades do Brasil como São Paulo, Rio de Janeiro, Recife, Porto Alegre, e em Buenos Aires e Santiago. Com esses projetos, geramos uma quantidade de dados muito grande que precisam ser coletados, tratados e disponibilizados pelo time de Dados. Para isso, buscamos profissionais apaixonad@s por dados, dur@s na queda - para enfrentar os desafios, inquiet@s - sempre buscando evoliuir e ousad@s - sem medo de errar. Espero que você tenha se identificado com os nossos objetivos e queira se juntar a nós!! Para isso elaboramos este teste. O teste deverá ser entregue em um repositório GIT de sua preferência, contendo um arquivo README com as respostas.

#### DESAFIO

``` Os dados a serem utilizados neste desafio se encontram neste repositório na pasta DATA ```

###### PARTE 01
Levando em conta duas fontes de dados sendo: um banco relacional e uma API de stream de dados, sua primeira missão será desenhar 
uma arquitetura orientada a EVENTOS para a ingestão near real-time destes dados em um datalake. Justifique 
brevemente sua escolha levantando os pontos positivos e negativos da solução.


###### PARTE 02


Seu objetivo é criar um script SPARK, que gere um arquivo único de output, utilizando os 3 arquivos em csv (station.csv, trip.csv e weather.csv), contendo todos os dados de viagens, acrescidos de:
- Latitude/Longitude da estação de início e de fim.
- Uma coluna com o nome "long_trip" de valor booleano sendo 'true' para viagens superiores a 10 minutos.
- Condição meteorológia no dia da viagem (coluna events da tabela weather).
- Uma coluna com o nome "age_range" sendo o valor de 1 para pessoas de 0-16 anos, 2 de 17-25, 3 de 26-50 e 4 para 50+.

``` O serviço de execução fica a seu critério, podendo utilizar tanto serviços locais como serviços em cloud. Justificar brevemente o serviço escolhido (EMR, Glue, Zeppelin, etc.) e o  formato do arquivo final. Incluir também o passo a passo de como executar o script.```

Qualquer dúvida estamos a disposição!

